// service.js (drop-in replacement)
// - Works with ONLY 3 tables: queue, matches, events (+ player_profiles you already use)
// - Match success -> auto create xl room -> save room_id + sides
// - Provides /api/room/join to return tencentmsdk deep link (auto blue/red)

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

// XL room endpoints
const ROOM_CREATE_URL = process.env.ROOM_CREATE_URL;
const ROOM_PAGE_PREFIX = process.env.ROOM_PAGE_PREFIX;

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

// ===== Rate limit (in-memory) + state cache =====
const RATE = {
  state_ip:   { windowMs: 10_000, limit: 10 }, // 10 req / 10s per IP
  state_wallet:{ windowMs: 10_000, limit: 4 }, // 4 req / 10s per wallet
  write_wallet:{ windowMs: 30_000, limit: 2 }, // 2 req / 30s per wallet (enqueue/leave)
  join_wallet: { windowMs: 30_000, limit: 3 }, // 3 req / 30s per wallet (room/join)
};

const buckets = new Map(); // key -> { resetAt, count }

function hitLimit(key, rule) {
  const now = Date.now();
  const b = buckets.get(key);
  if (!b || now > b.resetAt) {
    buckets.set(key, { resetAt: now + rule.windowMs, count: 1 });
    return false;
  }
  b.count += 1;
  return b.count > rule.limit;
}

function getClientIp(req) {
  // Render / Cloudflare 可能会带 x-forwarded-for
  const xf = req.headers["x-forwarded-for"];
  if (typeof xf === "string" && xf.length) return xf.split(",")[0].trim();
  return req.socket?.remoteAddress || "unknown";
}

// /api/state cache: wallet -> { expAt, jsonString }
const stateCache = new Map();
function cacheGet(wallet) {
  const v = stateCache.get(wallet);
  if (!v) return null;
  if (Date.now() > v.expAt) { stateCache.delete(wallet); return null; }
  return v.body;
}
function cacheSet(wallet, body, ttlMs = 2000) {
  stateCache.set(wallet, { expAt: Date.now() + ttlMs, body });
}


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
// XL room helpers
// ---------------------------

function buildRoomCs({ uid }) {
  return JSON.stringify({
    type: "zsf",
    mapID: 20001,
    mapType: 1,
    uid: String(uid),
    platType: "2",
    banhero: [],
    cs: []
  });
}

async function createXlRoom({ matchId }) {
  // 用 matchId 派生一个确定性的 uid（不依赖随机）
  const uid = 100000000000000000n + BigInt(matchId);
  const cs = buildRoomCs({ uid });

  const form = new URLSearchParams();
  form.set("cs", cs);
  form.set("roomName", "未命名房间");

  const resp = await fetch(ROOM_CREATE_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
    body: form.toString()
  });

  const text = (await resp.text()).trim();
  if (!resp.ok) throw new Error(`create room failed HTTP ${resp.status}: ${text}`);
  if (!/^\d{3,10}$/.test(text)) throw new Error(`unexpected roomId response: "${text}"`);

  return text; // roomId
}

function parseGamedataFromLaunchUrl(launchUrl) {
  // launchUrl: tencentmsdk1104466820://?gamedata=SmobaLaunch_xxx
  const idx = launchUrl.indexOf("gamedata=");
  if (idx < 0) throw new Error("missing gamedata");
  const gamedata = decodeURIComponent(launchUrl.slice(idx + "gamedata=".length));
  if (!gamedata.startsWith("SmobaLaunch_")) throw new Error("bad gamedata prefix");
  const b64 = gamedata.slice("SmobaLaunch_".length);
  return b64;
}

function decodePayloadStr(b64) {
  // 重要：不要 JSON.parse（会丢大整数精度）
  return Buffer.from(b64, "base64").toString("utf8");
}

function encodePayloadStr(payloadStr) {
  return Buffer.from(payloadStr, "utf8").toString("base64");
}

function setCampid(payloadStr, campid /* "1" or "2" */) {
  if (!/\"campid\"\s*:\s*\"[12]\"/.test(payloadStr)) {
    throw new Error("campid field not found in payload");
  }
  return payloadStr.replace(/\"campid\"\s*:\s*\"[12]\"/, `"campid":"${campid}"`);
}

function extractUllRoomid(payloadStr) {
  const m = payloadStr.match(/\"ullRoomid\"\s*:\s*(\d+)/);
  return m ? m[1] : null; // 作为字符串
}

function buildLaunchUrlFromPayloadStr(payloadStr) {
  const b64 = encodePayloadStr(payloadStr);
  const gamedata = `SmobaLaunch_${b64}`;
  return `tencentmsdk1104466820://?gamedata=${encodeURIComponent(gamedata)}`;
}


// 只在 matches.room_id 为空时创建并写入，避免重复覆盖
async function ensureRoomForMatch({ matchId, a, b }) {
  // 先查一下是否已经有 room
  const { data: existing, error: qerr } = await supabase
    .from("matches")
    .select("room_id")
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("chain_match_id", Number(matchId))
    .maybeSingle();

  if (qerr) throw qerr;
  if (existing?.room_id) return existing.room_id;

  const roomId = await createXlRoom({ matchId });

  // 固定分配：player_a=蓝，player_b=红（确定性，不会乱）
  // 只在 room_id is null 时写入，避免多实例覆盖
  const { data: upd, error: uerr } = await supabase
    .from("matches")
    .update({
      room_id: roomId,
      room_created_at: nowIso(),
      side_a: "blue",
      side_b: "red",
      updated_at: nowIso()
    })
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("chain_match_id", Number(matchId))
    .is("room_id", null)
    .select("room_id");

  if (uerr) throw uerr;

  // 如果 update 没更新到行，说明别的实例已经写了 room_id（我们创建了额外房间也无所谓，不影响主流程）
  const finalRoomId = upd?.[0]?.room_id || roomId;

  await logEvent("room_created", { matchId, roomId: finalRoomId, a, b }, null, matchId);
  console.log("[room] ensured roomId", finalRoomId, "for match", matchId);

  return finalRoomId;
}

// ---------------------------
// DB helpers
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
    .in("status", ["queued", "pending_enqueue"]);

  if (error) throw error;
}

async function rollbackQueueToQueued(a, b) {
  const { error } = await supabase
    .from("queue")
    .update({ status: "queued", updated_at: nowIso() })
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("wallet", [lc(a), lc(b)])
    .in("status", ["matched", "locking"]); // ✅ 放宽一点

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

  const { data: upd, error: uerr } = await supabase
    .from("queue")
    .update({ status: "matched", updated_at: nowIso() })
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("wallet", [a, b])
    .eq("status", "queued")
    .select("wallet");

  if (uerr) throw uerr;
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
  if (st === 1) return "locking";
  if (st === 2) return "disputed";
  if (st === 3) return "resolved";
  if (st === 4) return "resolved";
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

      // 回滚：让两人回 queued（确保 rollbackQueueToQueued 支持 matched/locking）
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

    // 2) 写 matches + queue
    try {
      await upsertMatch(locked.matchId, locked.a, locked.b, "locking");
      await markQueueMatched(locked.a, locked.b);
      await logEvent("match_locked", { matchId: locked.matchId, a: locked.a, b: locked.b }, null, locked.matchId);
      console.log("[match] ✅ wrote match + marked queue matched");

      // 3) 分配房间（失败不影响主流程）
      try {
        const { data: mm, error: mmErr } = await supabase
          .from("matches")
          .select("room_pool_id,launch_blue,launch_red")
          .eq("contract_address", CONTRACT_ADDRESS)
          .eq("chain_match_id", Number(locked.matchId))
          .maybeSingle();
        if (mmErr) throw mmErr;

        const already = mm && (mm.room_pool_id || (mm.launch_blue && mm.launch_red));
        if (!already) {
          const room = await claimRoomFromPool(locked.matchId);
          if (room) {
            console.log("[room] assigned to match", locked.matchId, "pool:", room.roomPoolId);
            await logEvent("room_assigned", { matchId: locked.matchId, roomPoolId: room.roomPoolId }, null, locked.matchId);
          } else {
            console.log("[room] no unused room available");
            await logEvent("room_empty", { matchId: locked.matchId }, null, locked.matchId);
          }
        } else {
          console.log("[room] already assigned for match", locked.matchId);
        }
      } catch (e) {
        console.error("[room] assign failed:", e?.message || e);
        await logEvent("room_assign_failed", { matchId: locked.matchId, err: e?.message || String(e) }, null, locked.matchId);
      }

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
// ---------------------------

let syncBusy = false;

async function scanRecentMatchLockedEvents() {
  const latest = await provider.getBlockNumber();
  const fromBase = Math.max(0, latest - PRUNED_WINDOW);
  const filter = contract.filters.MatchLocked();

  // 动态分段扫描：遇到 413 就自动缩小区间
  async function scanRange(from, to) {
    if (from > to) return;

    try {
      const logs = await contract.queryFilter(filter, from, to);

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

      return;
    } catch (e) {
      const msg = String(e?.shortMessage || e?.message || "").toLowerCase();

      // QuickNode / 一些节点会用 413 拒绝返回过大的 logs
      const is413 = msg.includes("413") || msg.includes("request entity too large");

      // 只有 413 才分裂；其它错误直接抛出去让外层打印
      if (!is413) throw e;

      // 区间太大：二分
      const mid = Math.floor((from + to) / 2);
      if (mid <= from) {
        // 已经缩到最小还 413，说明这个区块单块 logs 都太多（极少）
        console.error("[sync] getLogs still 413 even for tiny range", from, to);
        return;
      }

      console.warn("[sync] getLogs 413, split range:", from, to, "->", from, mid, "and", mid + 1, to);
      await scanRange(from, mid);
      await scanRange(mid + 1, to);
    }
  }

  // 这里不用固定 step 了，直接扫整个窗口，由 scanRange 自动拆
  await scanRange(fromBase, latest);
}


async function syncMatchStatuses() {
  const { data, error } = await supabase
    .from("matches")
    .select("chain_match_id,status")
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("status", ["locking", "disputed"])
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
      const winner = lc(res[9]);
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
    } catch {
      continue;
    }

    const want = im ? "matched" : (iq ? "queued" : "cancelled");

    if (row.status === "pending_enqueue" || row.status === "pending_leave") {
      if (row.status !== want) {
        await updateQueue(w, { status: want });
        await logEvent("queue_pending_resolved", { wallet: w, from: row.status, to: want }, w);
      }
      continue;
    }

    if (row.status !== want) {
      const c = (lastCheck.get(w) || 0) + 1;
      lastCheck.set(w, c);

      if (c < 2) continue;
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
    // await scanRecentMatchLockedEvents();
    await syncMatchStatuses();
    await fixQueueAndPending();
  } catch (e) {
    console.error("[sync] tick error:", e?.message || e);
  } finally {
    syncBusy = false;
  }
}

// ---------------------------
// Simple HTTP API
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

async function claimRoomFromPool(matchId) {
  const mid = Number(matchId);

  // 0) 先看看这局是否已经分配过（幂等：已分配就直接返回）
  const { data: existing, error: e0 } = await supabase
    .from("matches")
    .select("room_pool_id,launch_blue,launch_red")
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("chain_match_id", mid)
    .maybeSingle();

  if (e0) throw e0;

  if (existing?.launch_blue && existing?.launch_red) {
    return {
      roomPoolId: existing.room_pool_id || null,
      launchBlue: existing.launch_blue,
      launchRed: existing.launch_red,
      ullRoomid: null
    };
  }

  // 1) 取一条 unused（尽量最老的）
  const { data: rows, error: qErr } = await supabase
    .from("room_pool")
    .select("id,launch_blue,launch_red,ull_roomid")
    .eq("status", "unused")
    .order("created_at", { ascending: true })
    .limit(1);

  if (qErr) throw qErr;
  const r = rows?.[0];
  if (!r) return null;

  // 2) 抢占：把 room_pool 标 used（避免并发重复用）
  const { data: upd, error: uErr } = await supabase
    .from("room_pool")
    .update({ status: "used", used_at: nowIso(), used_by_wallet: null })
    .eq("id", r.id)
    .eq("status", "unused")
    .select("id");

  if (uErr) throw uErr;
  if (!upd || upd.length === 0) return null; // 被别人抢了，外层可重试

  // 3) 写入 matches：只允许“未分配”的局写入，避免重复 join 覆盖
  const { data: mUpd, error: mErr } = await supabase
    .from("matches")
    .update({
      room_pool_id: r.id,
      launch_blue: r.launch_blue,
      launch_red: r.launch_red,
      room_assigned_at: nowIso()
    })
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("chain_match_id", mid)
    .is("room_pool_id", null)          // ✅ 关键：只在未分配时才允许更新
    .is("launch_blue", null)
    .is("launch_red", null)
    .select("room_pool_id,launch_blue,launch_red");

  if (mErr) {
    // matches 更新失败：尽量把 room_pool 回收
    try {
      await supabase
        .from("room_pool")
        .update({ status: "unused", used_at: null, used_by_wallet: null })
        .eq("id", r.id)
        .eq("status", "used");
    } catch {}
    throw mErr;
  }

  // 如果 0 行被更新，说明这局刚刚已经被别人分配了
  if (!mUpd || mUpd.length === 0) {
    // 回收我们刚抢到的 pool（避免浪费）
    try {
      await supabase
        .from("room_pool")
        .update({ status: "unused", used_at: null, used_by_wallet: null })
        .eq("id", r.id)
        .eq("status", "used");
    } catch {}

    // 再读一次 matches，把已经分配好的返回
    const { data: again, error: e2 } = await supabase
      .from("matches")
      .select("room_pool_id,launch_blue,launch_red")
      .eq("contract_address", CONTRACT_ADDRESS)
      .eq("chain_match_id", mid)
      .maybeSingle();
    if (e2) throw e2;

    if (again?.launch_blue && again?.launch_red) {
      return {
        roomPoolId: again.room_pool_id || null,
        launchBlue: again.launch_blue,
        launchRed: again.launch_red,
        ullRoomid: null
      };
    }

    // 理论上很少：刚好还没写上
    return null;
  }

  // 正常返回
  return {
    roomPoolId: r.id,
    launchBlue: r.launch_blue,
    launchRed: r.launch_red,
    ullRoomid: r.ull_roomid
  };
}



async function handle(req, res) {
  setCors(res);
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  const u = new URL(req.url, "http://localhost");
  const path = u.pathname;
  const ip = getClientIp(req);

  try {
    if (req.method === "GET" && (path === "/" || path === "/health")) {
      res.writeHead(200, { "Content-Type": "text/plain" });
      return res.end("matchmaker running\n");
    }

    // ---- profile apis (your existing code kept) ----
    if (req.method === "POST" && path === "/api/profile/set_once") {
      const { wallet: w, gameName, message, signature } = await readJson(req);

      if (!w || !gameName || !message || !signature) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "missing wallet/gameName/message/signature" }));
      }

      const walletAddr = lc(w);
      const name = String(gameName).trim();

      if (name.length < 1 || name.length > 30) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "gameName length must be 1~30" }));
      }

      const lines = String(message).split("\n").map(s => s.trim()).filter(Boolean);
      if (lines[0] !== "WZRY_SET_NAME_ONCE") {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "bad message header" }));
      }
      const kv = {};
      for (const line of lines.slice(1)) {
        const idx = line.indexOf(":");
        if (idx > 0) kv[line.slice(0, idx).toLowerCase()] = line.slice(idx + 1);
      }

      const msgWallet = lc(kv.wallet || "");
      const msgName = String(kv.name || "").trim();
      const ts = Number(kv.ts || 0);
      const origin = String(kv.origin || "");
      const msgContract = lc(kv.contract || "");

      if (msgWallet !== walletAddr || msgName !== name || msgContract !== CONTRACT_ADDRESS || !ts || !origin) {
        res.writeHead(403, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "message mismatch" }));
      }

      const now = Date.now();
      if (Math.abs(now - ts) > 5 * 60 * 1000) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "signature expired" }));
      }

      let recovered;
      try {
        recovered = ethers.verifyMessage(message, signature);
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "bad signature" }));
      }
      if (lc(recovered) !== walletAddr) {
        res.writeHead(403, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "signature not from wallet" }));
      }

      const { data: exist, error: e1 } = await supabase
        .from("player_profiles")
        .select("wallet,game_name")
        .eq("wallet", walletAddr)
        .maybeSingle();

      if (e1) {
        res.writeHead(500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: e1.message }));
      }
      if (exist) {
        res.writeHead(409, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "name already set and cannot be changed" }));
      }

      const { error: insErr } = await supabase
        .from("player_profiles")
        .insert({ wallet: walletAddr, game_name: name });

      if (insErr) {
        res.writeHead(500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: insErr.message }));
      }

      await logEvent("set_name_once", { name, origin, ts }, walletAddr);

      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ ok: true, gameName: name }));
    }

    if (req.method === "GET" && path === "/api/profile/get") {
      const w = lc(u.searchParams.get("wallet") || "");
      if (!w) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "missing wallet" }));
      }

      const { data, error } = await supabase
        .from("player_profiles")
        .select("wallet,game_name,created_at")
        .eq("wallet", w)
        .maybeSingle();

      if (error) {
        res.writeHead(500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: error.message }));
      }

      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ ok: true, profile: data || null }));
    }

    // ---- queue apis ----
if (req.method === "POST" && path === "/api/queue/enqueue") {
  const { wallet: w, txHash } = await readJson(req);
  if (!w || !txHash) {
    res.writeHead(400, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "missing wallet/txHash" }));
  }

  const wallet = lc(w);

  // ✅ 限流：同一钱包 30 秒最多 2 次写操作（enqueue/leave 共用）
  if (hitLimit(`w:write:${wallet}`, RATE.write_wallet)) {
    res.writeHead(429, { "Content-Type": "application/json", "Retry-After": "10" });
    return res.end(JSON.stringify({ ok: false, error: "rate limited" }));
  }

  await upsertQueue(wallet, { status: "pending_enqueue", enqueue_tx: txHash });
  await logEvent("api_enqueue", { txHash }, wallet);

  res.writeHead(200, { "Content-Type": "application/json" });
  return res.end(JSON.stringify({ ok: true }));
}

if (req.method === "POST" && path === "/api/queue/leave") {
  const { wallet: w, txHash } = await readJson(req);
  if (!w || !txHash) {
    res.writeHead(400, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "missing wallet/txHash" }));
  }

  const wallet = lc(w);

  // ✅ 限流：同一钱包 30 秒最多 2 次写操作（enqueue/leave 共用）
  if (hitLimit(`w:write:${wallet}`, RATE.write_wallet)) {
    res.writeHead(429, { "Content-Type": "application/json", "Retry-After": "10" });
    return res.end(JSON.stringify({ ok: false, error: "rate limited" }));
  }

  await upsertQueue(wallet, { status: "pending_leave", leave_tx: txHash });
  await logEvent("api_leave", { txHash }, wallet);

  res.writeHead(200, { "Content-Type": "application/json" });
  return res.end(JSON.stringify({ ok: true }));
}

// ---- room join ----
if (req.method === "GET" && path === "/api/room/join") {
  const w = lc(u.searchParams.get("wallet") || "");
  if (!w) {
    res.writeHead(400, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "missing wallet" }));
  }

  // ✅ 限流：同一钱包 30 秒最多 3 次 join（防狂点）
  if (hitLimit(`w:join:${w}`, RATE.join_wallet)) {
    res.writeHead(429, { "Content-Type": "application/json", "Retry-After": "10" });
    return res.end(JSON.stringify({ ok: false, error: "rate limited" }));
  }

  // 找我参与的、进行中的 match（只认进行中）
  const { data: ms, error: mErr } = await supabase
    .from("matches")
    .select("chain_match_id,player_a,player_b,status,room_pool_id,launch_blue,launch_red")
    .eq("contract_address", CONTRACT_ADDRESS)
    .or(`player_a.eq.${w},player_b.eq.${w}`)
    .in("status", ["locking", "disputed"])
    .order("created_at", { ascending: false })
    .limit(1);

  if (mErr) throw mErr;

  const m = ms?.[0];
  if (!m) {
    res.writeHead(404, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "no active match" }));
  }

  // A 蓝 B 红（固定）
  const isA = lc(m.player_a) === w;
  const side = isA ? "blue" : "red";

  // ✅ 1) 已经有 deeplink：直接返回（重复点击不会再分配）
  if (m.launch_blue && m.launch_red) {
    const launchUrl = side === "blue" ? m.launch_blue : m.launch_red;
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({
      ok: true,
      matchId: Number(m.chain_match_id),
      side,
      launchUrl,
      reused: true
    }));
  }

  // ✅ 2) 没有才尝试 claim（claimRoomFromPool 内部已做并发安全）
  const room = await claimRoomFromPool(Number(m.chain_match_id));
  if (!room || !room.launchBlue || !room.launchRed) {
    res.writeHead(409, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "room not ready yet (pool empty or assigning)" }));
  }

  const launchUrl = side === "blue" ? room.launchBlue : room.launchRed;

  res.writeHead(200, { "Content-Type": "application/json" });
  return res.end(JSON.stringify({
    ok: true,
    matchId: Number(m.chain_match_id),
    side,
    launchUrl,
    reused: false
  }));
}

    // ---- match history (paged) ----
// GET /api/match/history?wallet=0x...&page=1&pageSize=10
if (req.method === "GET" && path === "/api/match/history") {
  const w = lc(u.searchParams.get("wallet") || "");
  if (!w) {
    res.writeHead(400, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "missing wallet" }));
  }

  // page/pageSize
  const page = Math.max(1, Number(u.searchParams.get("page") || 1));
  const pageSizeRaw = Number(u.searchParams.get("pageSize") || 10);
  const pageSize = Math.min(50, Math.max(1, pageSizeRaw)); // 最多 50/页，防止被刷爆
  const from = (page - 1) * pageSize;
  const to = from + pageSize - 1;

  // 1) 查 matches（只取已结束 resolved）
  const { data: ms, error: mErr, count } = await supabase
    .from("matches")
    .select(
      "chain_match_id,player_a,player_b,status,winner,created_at",
      { count: "exact" }
    )
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("status", "resolved")
    .or(`player_a.eq.${w},player_b.eq.${w}`)
    .order("created_at", { ascending: false })
    .range(from, to);

  if (mErr) throw mErr;

  const matches = ms || [];
  if (matches.length === 0) {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({
      ok: true,
      page,
      pageSize,
      total: count || 0,
      rows: []
    }));
  }

  // 2) 批量取 profiles（我 + 对手们）
  const wallets = new Set();
  wallets.add(w);
  for (const m of matches) {
    wallets.add(lc(m.player_a));
    wallets.add(lc(m.player_b));
  }
  const walletList = Array.from(wallets);

  const { data: ps, error: pErr } = await supabase
    .from("player_profiles")
    .select("wallet,game_name")
    .in("wallet", walletList);

  if (pErr) throw pErr;

  const nameMap = new Map();
  for (const p of (ps || [])) {
    nameMap.set(lc(p.wallet), p.game_name);
  }

  // 3) 组装 rows
  const rows = matches.map(m => {
    const a = lc(m.player_a);
    const b = lc(m.player_b);
    const opp = (a === w) ? b : a;

    const meName = nameMap.get(w) || w;
    const oppName = nameMap.get(opp) || opp;

    const winner = lc(m.winner || "");
    let result = "unknown";
    if (!winner || winner === lc(ethers.ZeroAddress)) {
      // winner 为空/零地址：你可以认为是未知或平局
      result = "unknown";
    } else {
      result = (winner === w) ? "win" : "lose";
    }

    return {
      chain_match_id: Number(m.chain_match_id),
      result,
      me: meName,
      opponent: oppName,
      opponent_wallet: opp,
      created_at: m.created_at
    };
  });

  res.writeHead(200, { "Content-Type": "application/json" });
  return res.end(JSON.stringify({
    ok: true,
    page,
    pageSize,
    total: count || 0,
    rows
  }));
}


    if (req.method === "POST" && path === "/api/roompool/add") {
      const { launchUrl } = await readJson(req);
      if (!launchUrl) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: false, error: "missing launchUrl" }));
      }

      const b64 = parseGamedataFromLaunchUrl(String(launchUrl).trim());
      const payloadStr = decodePayloadStr(b64);

      // 自动补齐两边
      const payloadBlue = setCampid(payloadStr, "1");
      const payloadRed  = setCampid(payloadStr, "2");

      const launch_blue = buildLaunchUrlFromPayloadStr(payloadBlue);
      const launch_red  = buildLaunchUrlFromPayloadStr(payloadRed);

      const ullRoomid = extractUllRoomid(payloadStr);

      const { error } = await supabase.from("room_pool").insert({
        ull_roomid: ullRoomid,
        base_payload: payloadStr,
        launch_blue,
        launch_red,
        status: "unused"
      });

      if (error) throw error;

      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ ok: true, ullRoomid, launch_blue, launch_red }));
    }

    // ---- state ----
    if (req.method === "GET" && path === "/api/state") {
  const w = lc(u.searchParams.get("wallet") || "");
  if (!w) {
    res.writeHead(400, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: false, error: "missing wallet" }));
  }

  // rate limit
  if (hitLimit(`ip:state:${ip}`, RATE.state_ip) || hitLimit(`w:state:${w}`, RATE.state_wallet)) {
    res.writeHead(429, { "Content-Type": "application/json", "Retry-After": "5" });
    return res.end(JSON.stringify({ ok: false, error: "rate limited" }));
  }

  // cache
  const cached = cacheGet(w);
  if (cached) {
    res.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "private, max-age=2"
    });
    return res.end(cached);
  }

  const { data: q, error: qErr } = await supabase
    .from("queue")
    .select("status,enqueue_tx,leave_tx,updated_at")
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("wallet", w)
    .maybeSingle();
  if (qErr) throw qErr;

  const { data: ms, error: mErr } = await supabase
    .from("matches")
    .select("chain_match_id,player_a,player_b,status,winner,dispute_by,updated_at,room_pool_id,launch_blue,launch_red")
    .eq("contract_address", CONTRACT_ADDRESS)
    .or(`player_a.eq.${w},player_b.eq.${w}`)
    .in("status", ["locking", "disputed"]) // ✅ 只返回进行中，避免“乱跳回旧对局”
    .order("created_at", { ascending: false })
    .limit(1);
  if (mErr) throw mErr;

  const { data: meProf, error: meErr } = await supabase
    .from("player_profiles")
    .select("wallet,game_name")
    .eq("wallet", w)
    .maybeSingle();
  if (meErr) throw meErr;

  let oppProf = null;
  const m = (ms && ms[0]) ? ms[0] : null;
  if (m) {
    const a = lc(m.player_a);
    const b = lc(m.player_b);
    const oppWallet = (a === w) ? b : a;

    const { data: o, error: oErr } = await supabase
      .from("player_profiles")
      .select("wallet,game_name")
      .eq("wallet", oppWallet)
      .maybeSingle();
    if (oErr) throw oErr;
    oppProf = o || null;
  }

  const bodyObj = {
    ok: true,
    queue: q || null,
    match: (ms && ms[0]) || null,
    meProfile: meProf || null,
    opponentProfile: oppProf
  };
  const body = JSON.stringify(bodyObj);

  cacheSet(w, body, 2000);

  res.writeHead(200, {
    "Content-Type": "application/json",
    "Cache-Control": "private, max-age=2"
  });
  return res.end(body);
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
console.log("ROOM_CREATE_URL:", ROOM_CREATE_URL);
console.log("ROOM_PAGE_PREFIX:", ROOM_PAGE_PREFIX);

http.createServer(handle).listen(PORT, () => {
  console.log("HTTP server listening on", PORT);
});

setInterval(tickMatch, MATCH_INTERVAL_MS);
setInterval(tickSync, SYNC_INTERVAL_MS);

// 立即执行一次
tickMatch();
tickSync();
