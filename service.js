// service.js (drop-in replacement)
// - Works with ONLY 3 tables: queue, matches, events
// - No RPC required, no chain_sync_state required
// - Provides simple HTTP APIs for frontend write requests
// - Runs matchmaker loop + chain sync loop

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { ethers } from "ethers";
import http from "http";
import { URL } from "url";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
const RPC_URL = mustEnv("RPC_URL");
const CONTRACT_ADDRESS = mustEnv("CONTRACT_ADDRESS").toLowerCase();
const OPERATOR_PRIVATE_KEY = mustEnv("OPERATOR_PRIVATE_KEY");

const MATCH_INTERVAL_MS = Number(process.env.MATCH_INTERVAL_MS || 3000);
const SYNC_INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS || 8000);
const SYNC_BATCH = Number(process.env.SYNC_BATCH || 50);

// 每次同步从最近窗口扫 MatchLocked（避免需要 chain_sync_state 表）
const EVENT_SCAN_STEP = Number(process.env.EVENT_SCAN_STEP || 20000);
const PRUNED_WINDOW = Number(process.env.PRUNED_WINDOW || 60000); // 最近多少块

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const ABI = [
  "function lockMatch(address a, address b)",
  "function inQueue(address) view returns (bool)",
  "function inMatch(address) view returns (bool)",
  "function getMatch(uint256 matchId) view returns (address a,address b,uint8 status,uint8 reportA,uint8 reportB,address winReporter,uint64 confirmDeadline,address disputedBy,bytes32 disputeReasonHash,address winner)",
  "event MatchLocked(uint256 indexed matchId, address indexed a, address indexed b)"
];

const provider = new ethers.JsonRpcProvider(RPC_URL, undefined, {
  polling: true,
  staticNetwork: ethers.Network.from(56)
});
const wallet = new ethers.Wallet(OPERATOR_PRIVATE_KEY, provider);
const contract = new ethers.Contract(CONTRACT_ADDRESS, ABI, wallet);

function lc(addr) { return (addr || "").toLowerCase(); }
function nowIso() { return new Date().toISOString(); }

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function logEvent(type, payload = {}, walletAddr = null, matchId = null) {
  try {
    await supabase.from("events").insert({
      type,
      wallet: walletAddr ? lc(walletAddr) : null,
      match_id: matchId ?? null,
      contract_address: CONTRACT_ADDRESS,
      payload
    });
  } catch {}
}

// ---------------------------
// DB helpers (new schema)
// queue: wallet, contract_address, status, enqueue_tx, leave_tx, created_at, updated_at
// matches: contract_address, chain_match_id, player_a, player_b, status, winner, dispute_by, created_at, updated_at
// ---------------------------

async function upsertQueue(walletAddr, patch) {
  const payload = {
    wallet: lc(walletAddr),
    contract_address: CONTRACT_ADDRESS,
    updated_at: nowIso(),
    ...patch
  };

  const { error } = await supabase
    .from("queue")
    .upsert(payload, { onConflict: "wallet,contract_address" });

  if (error) throw error;
}

async function updateQueue(walletAddr, patch) {
  const { error } = await supabase
    .from("queue")
    .update({ ...patch, updated_at: nowIso() })
    .eq("wallet", lc(walletAddr))
    .eq("contract_address", CONTRACT_ADDRESS);

  if (error) throw error;
}

async function markQueueMatched(a, b) {
  const { error } = await supabase
    .from("queue")
    .update({ status: "matched", updated_at: nowIso() })
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("wallet", [lc(a), lc(b)])
    .in("status", ["queued", "pending_enqueue"]); // 保险：避免误伤 cancelled/pending_leave

  if (error) throw error;
}

async function rollbackQueueToQueued(a, b) {
  const { error } = await supabase
    .from("queue")
    .update({ status: "queued", updated_at: nowIso() })
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("wallet", [lc(a), lc(b)])
    .in("status", ["matched"]); // 只回滚我们刚刚 claim 的

  if (error) throw error;
}

async function upsertMatch(matchId, a, b, statusText, extra = {}) {
  const payload = {
    contract_address: CONTRACT_ADDRESS,
    chain_match_id: Number(matchId),
    player_a: lc(a),
    player_b: lc(b),
    status: statusText,
    updated_at: nowIso(),
    ...extra
  };

  const { error } = await supabase
    .from("matches")
    .upsert(payload, { onConflict: "contract_address,chain_match_id" });

  if (error) throw error;
}

async function updateMatch(matchId, patch) {
  const { error } = await supabase
    .from("matches")
    .update({ ...patch, updated_at: nowIso() })
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("chain_match_id", Number(matchId));

  if (error) throw error;
}

// “在 DB 里选 2 个 queued，尽量原子 claim（单实例够用）”
async function claimTwoPlayers() {
  // 取最早的 2 个 queued
  const { data: rows, error } = await supabase
    .from("queue")
    .select("wallet,status,created_at")
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("status", "queued")
    .order("created_at", { ascending: true })
    .limit(2);

  if (error) throw error;
  if (!rows || rows.length < 2) return null;

  const a = lc(rows[0].wallet);
  const b = lc(rows[1].wallet);

  // 把这两个人标为 matched，避免下一个 tick 又拿到（这里用 status=queued 限制）
  const { data: upd, error: uerr } = await supabase
    .from("queue")
    .update({ status: "matched", updated_at: nowIso() })
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("wallet", [a, b])
    .eq("status", "queued")
    .select("wallet");

  if (uerr) throw uerr;

  // 如果更新不到 2 行，说明被别的流程抢了（或状态变了）
  if (!upd || upd.length < 2) return null;

  return { a, b };
}

// ---------------------------
// Chain helpers
// ---------------------------

function extractMatchLocked(receipt) {
  for (const log of receipt.logs || []) {
    try {
      const parsed = contract.interface.parseLog(log);
      if (parsed?.name === "MatchLocked") {
        return {
          matchId: Number(parsed.args.matchId),
          a: lc(parsed.args.a),
          b: lc(parsed.args.b)
        };
      }
    } catch {}
  }
  return null;
}

function mapChainStatusToText(st) {
  // 你合约注释：1 locked,2 disputed,3 settled,4 cancelled
  // 我们表里是 locking/playing/disputed/resolved
  if (st === 1) return "locking";
  if (st === 2) return "disputed";
  if (st === 3) return "resolved";
  if (st === 4) return "resolved"; // 取消也归到 resolved（你也可以以后加 cancelled 状态）
  return "locking";
}

// ---------------------------
// MATCH LOOP
// ---------------------------

let matchBusy = false;
async function tickMatch() {
  if (matchBusy) return;
  matchBusy = true;

  try {
    await provider.getBlockNumber(); // RPC health

    const pair = await claimTwoPlayers();
    if (!pair) return;

    const { a, b } = pair;
    console.log("[match] claimed pair:", a, b);
    await logEvent("claim_pair", { a, b });

    // 可选链上校验：DB queued 但链上不在队列，直接纠偏
    const [aq, bq] = await Promise.all([contract.inQueue(a), contract.inQueue(b)]);
    if (!aq) {
      await updateQueue(a, { status: "cancelled" });
      await updateQueue(b, { status: "queued" });
      await logEvent("claim_pair_onchain_miss", { who: "a", a, b });
      return;
    }
    if (!bq) {
      await updateQueue(b, { status: "cancelled" });
      await updateQueue(a, { status: "queued" });
      await logEvent("claim_pair_onchain_miss", { who: "b", a, b });
      return;
    }

    // 1) 上链 lockMatch
    let receipt;
    try {
      const tx = await contract.lockMatch(a, b);
      console.log("[match] lockMatch tx:", tx.hash);
      await logEvent("lockMatch_tx", { a, b, txHash: tx.hash });

      receipt = await tx.wait();
    } catch (e) {
      console.error("[match] lockMatch failed:", e?.shortMessage || e?.message || e);
      // 上链失败：回滚 DB，让两人回队列
      await rollbackQueueToQueued(a, b);
      await logEvent("lockMatch_failed", { a, b, err: e?.shortMessage || e?.message || String(e) });
      return;
    }

    const locked = extractMatchLocked(receipt);
    if (!locked) {
      console.error("[match] tx ok but no MatchLocked parsed; rely on scan window");
      await logEvent("lockMatch_no_event", { a, b });
      return;
    }

    console.log("[match] locked matchId:", locked.matchId);

    // 2) 写 matches（上链成功后写库失败也不要回滚）
    try {
      await upsertMatch(locked.matchId, locked.a, locked.b, "locking");
      await markQueueMatched(locked.a, locked.b);
      await logEvent("match_locked", { matchId: locked.matchId, a: locked.a, b: locked.b }, null, locked.matchId);
      console.log("[match] ✅ wrote match + marked queue matched");
    } catch (dbErr) {
      console.error("[match] DB write failed AFTER lockMatch:", dbErr?.message || dbErr);
      await logEvent("db_write_failed_after_lock", { matchId: locked.matchId, err: dbErr?.message || String(dbErr) });
    }
  } catch (e) {
    console.error("[match] tick error:", e?.message || e);
  } finally {
    matchBusy = false;
  }
}

// ---------------------------
// SYNC LOOP
// 1) scan recent MatchLocked logs to backfill
// 2) sync match statuses from chain
// 3) fix queue inconsistencies + confirm pending statuses
// ---------------------------

let syncBusy = false;

async function scanRecentMatchLockedEvents() {
  const latest = await provider.getBlockNumber();
  const fromBase = Math.max(0, latest - PRUNED_WINDOW);
  const filter = contract.filters.MatchLocked();

  for (let from = fromBase; from <= latest; from += EVENT_SCAN_STEP + 1) {
    const to = Math.min(latest, from + EVENT_SCAN_STEP);

    let logs = [];
    try {
      logs = await contract.queryFilter(filter, from, to);
    } catch (e) {
      const msg = (e?.shortMessage || e?.message || "").toLowerCase();
      console.error("[sync] getLogs failed range", from, to, msg);
      return;
    }

    for (const ev of logs) {
      const matchId = Number(ev.args.matchId);
      const a = lc(ev.args.a);
      const b = lc(ev.args.b);

      try {
        await upsertMatch(matchId, a, b, "locking");
        await markQueueMatched(a, b);
      } catch (err) {
        console.error("[sync] backfill failed:", matchId, err?.message || err);
      }
    }
  }
}

async function syncMatchStatuses() {
  const { data, error } = await supabase
    .from("matches")
    .select("chain_match_id,status")
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("status", ["locking", "disputed"]) // 你现在主要关心这俩
    .order("created_at", { ascending: true })
    .limit(SYNC_BATCH);

  if (error) throw error;
  if (!data || data.length === 0) return;

  for (const row of data) {
    const mid = Number(row.chain_match_id);
    let res;
    try {
      res = await contract.getMatch(mid);
    } catch (e) {
      console.error("[sync] getMatch rpc fail mid", mid, e?.message || e);
      continue;
    }

    const chainSt = Number(res[2]);
    const want = mapChainStatusToText(chainSt);

    if (want !== row.status) {
      console.log("[sync] match", mid, row.status, "->", want);
      const winner = lc(res[9]); // winner address (per your ABI)
      const disputedBy = lc(res[7]);

      const patch = { status: want };
      if (want === "resolved" && winner && winner !== ethers.ZeroAddress) patch.winner = winner;
      if (want === "disputed" && disputedBy && disputedBy !== ethers.ZeroAddress) patch.dispute_by = disputedBy;

      await updateMatch(mid, patch);
      await logEvent("match_status_sync", { mid, from: row.status, to: want }, null, mid);
    }
  }
}

const lastCheck = new Map(); // wallet -> count

async function fixQueueAndPending() {
  // 处理：pending_enqueue / pending_leave / matched / cancelled / queued 的纠偏
  const { data, error } = await supabase
    .from("queue")
    .select("wallet,status")
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("status", ["pending_enqueue", "pending_leave", "matched", "cancelled", "queued"])
    .limit(SYNC_BATCH);

  if (error) throw error;
  if (!data || data.length === 0) return;

  for (const row of data) {
    const w = lc(row.wallet);

    let im, iq;
    try {
      [im, iq] = await Promise.all([
        contract.inMatch(w),
        contract.inQueue(w)
      ]);
    } catch (e) {
      continue;
    }

    // 规则：
    // - 链上 inMatch => matched
    // - 链上 inQueue => queued
    // - 都不是 => cancelled
    const want = im ? "matched" : (iq ? "queued" : "cancelled");

    // pending 状态：如果链上已经体现了，就落到最终状态
    if (row.status === "pending_enqueue" || row.status === "pending_leave") {
      if (row.status !== want) {
        await updateQueue(w, { status: want });
        await logEvent("queue_pending_resolved", { wallet: w, from: row.status, to: want }, w);
      }
      continue;
    }

    // 非 pending：做“二次确认”避免偶发 RPC 抖动
    if (row.status !== want) {
      const c = (lastCheck.get(w) || 0) + 1;
      lastCheck.set(w, c);

      if (c < 2) continue; // 连续两次不一致才改
      lastCheck.delete(w);

      await updateQueue(w, { status: want });
      await logEvent("queue_fixed", { wallet: w, from: row.status, to: want }, w);
    } else {
      lastCheck.delete(w);
    }
  }
}

async function tickSync() {
  if (syncBusy) return;
  syncBusy = true;

  try {
    await scanRecentMatchLockedEvents();
    await syncMatchStatuses();
    await fixQueueAndPending();
  } catch (e) {
    console.error("[sync] tick error:", e?.message || e);
  } finally {
    syncBusy = false;
  }
}

// ---------------------------
// Simple HTTP API for frontend
// - POST /api/queue/enqueue { wallet, txHash }
// - POST /api/queue/leave   { wallet, txHash }
// - GET  /api/state?wallet=0x...
// ---------------------------

function setCors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

async function readJson(req) {
  return await new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      if (!body) return resolve({});
      try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
    });
  });
}

async function handle(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  const u = new URL(req.url, "http://localhost");
  const path = u.pathname;

  try {
    if (req.method === "GET" && (path === "/" || path === "/health")) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      return res.end("matchmaker running\n");
    }

    if (req.method === "POST" && path === "/api/queue/enqueue") {
      const { wallet: w, txHash } = await readJson(req);
      if (!w || !txHash) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "missing wallet/txHash" }));
      }

      await upsertQueue(w, {
        status: "pending_enqueue",
        enqueue_tx: txHash
      });
      await logEvent("api_enqueue", { txHash }, w);

      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ ok: true }));
    }

    if (req.method === "POST" && path === "/api/queue/leave") {
      const { wallet: w, txHash } = await readJson(req);
      if (!w || !txHash) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "missing wallet/txHash" }));
      }

      // leave 也走 pending，最终由 sync 纠偏到 cancelled/queued/matched
      await upsertQueue(w, {
        status: "pending_leave",
        leave_tx: txHash
      });
      await logEvent("api_leave", { txHash }, w);

      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ ok: true }));
    }

    if (req.method === "GET" && path === "/api/state") {
      const w = lc(u.searchParams.get("wallet") || "");
      if (!w) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "missing wallet" }));
      }

      const { data: q, error: qErr } = await supabase
        .from("queue")
        .select("status,enqueue_tx,leave_tx,updated_at")
        .eq("contract_address", CONTRACT_ADDRESS)
        .eq("wallet", w)
        .maybeSingle();

      if (qErr) throw qErr;

      // 找这个人相关的一场“未 resolved”的对局（如果有）
      const { data: ms, error: mErr } = await supabase
        .from("matches")
        .select("chain_match_id,player_a,player_b,status,winner,dispute_by,updated_at")
        .eq("contract_address", CONTRACT_ADDRESS)
        .or(`player_a.eq.${w},player_b.eq.${w}`)
        .neq("status", "resolved")
        .order("created_at", { ascending: false })
        .limit(1);

      if (mErr) throw mErr;

      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ ok: true, queue: q || null, match: (ms && ms[0]) || null }));
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "not found" }));
  } catch (e) {
    console.error("[http] error:", e?.message || e);
    res.writeHead(500, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: e?.message || String(e) }));
  }
}

// ---------------------------
// Boot
// ---------------------------

const PORT = process.env.PORT || 10000;

console.log("Starting service...");
console.log("CONTRACT_ADDRESS:", CONTRACT_ADDRESS);
console.log("MATCH_INTERVAL_MS:", MATCH_INTERVAL_MS, "SYNC_INTERVAL_MS:", SYNC_INTERVAL_MS);

http.createServer(handle).listen(PORT, () => {
  console.log("HTTP server listening on", PORT);
});

setInterval(tickMatch, MATCH_INTERVAL_MS);
setInterval(tickSync, SYNC_INTERVAL_MS);

// 立即执行一次
tickMatch();
tickSync();
