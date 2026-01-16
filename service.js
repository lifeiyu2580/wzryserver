import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { ethers } from "ethers";
import http from "http";

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
const SYNC_INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS || 60000);
const SYNC_BATCH = Number(process.env.SYNC_BATCH || 50);
const EVENT_SCAN_BLOCKS = Number(process.env.EVENT_SCAN_BLOCKS || 2000); // 初次启动回扫多少块
const QUEUE_FIX_BATCH = Number(process.env.QUEUE_FIX_BATCH || 50);

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

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function lc(addr) { return (addr || "").toLowerCase(); }

// ===== DB helpers =====
async function claimTwoPlayers() {
  const { data, error } = await supabase.rpc("claim_two_players");
  if (error) throw error;
  if (!data || data.length === 0) return null;

  // 你函数返回字段名可能是 a_wallet/b_wallet 或 a/b，这里兼容一下
  const row = data[0];
  const a = lc(row.a_wallet || row.a);
  const b = lc(row.b_wallet || row.b);
  if (!a || !b) return null;

  return { a, b };
}

async function rollbackQueueToQueued(a, b) {
  await supabase
    .from("queue")
    .update({ status: "queued", updated_at: new Date().toISOString() })
    .in("wallet", [a, b]);
}

async function setQueueStatus(walletAddr, status) {
  await supabase
    .from("queue")
    .update({ status, updated_at: new Date().toISOString() })
    .eq("wallet", walletAddr);
}

async function upsertMatchFromChain(matchId, a, b, statusText) {
  const payload = {
    chain_match_id: Number(matchId),
    a_wallet: lc(a),
    b_wallet: lc(b),
    status: statusText,
    contract_address: CONTRACT_ADDRESS
  };

  // upsert: 如果你 matches 没 unique 约束，会插重复；建议你在 (contract_address, chain_match_id) 上做 unique
  // 这里先用 “先查再插/更” 的方式，避免表结构没改导致 upsert 报错。
  const { data: existing, error: qerr } = await supabase
    .from("matches")
    .select("id,status")
    .eq("contract_address", CONTRACT_ADDRESS)
    .eq("chain_match_id", Number(matchId))
    .limit(1);

  if (qerr) throw qerr;

  if (!existing || existing.length === 0) {
    const { error: ierr } = await supabase.from("matches").insert(payload);
    if (ierr) throw ierr;
  } else {
    // 状态不一样才更新，减少写库
    if (existing[0].status !== statusText) {
      const { error: uerr } = await supabase
        .from("matches")
        .update({ status: statusText })
        .eq("id", existing[0].id);
      if (uerr) throw uerr;
    }
  }
}

async function markMatched(a, b) {
  // 注意：只允许在链上确定 lockMatch 成功后调用
  const { error } = await supabase
    .from("queue")
    .update({ status: "matched", updated_at: new Date().toISOString() })
    .in("wallet", [a, b]);
  if (error) throw error;
}

async function updateMatchStatus(matchId, statusText) {
  await supabase
    .from("matches")
    .update({ status: statusText })
    .eq("chain_match_id", Number(matchId))
    .eq("contract_address", CONTRACT_ADDRESS);
}

// ===== chain sync state =====
async function getLastBlock() {
  const { data, error } = await supabase
    .from("chain_sync_state")
    .select("last_block")
    .eq("contract_address", CONTRACT_ADDRESS)
    .limit(1);

  if (error) throw error;

  if (!data || data.length === 0) return 0;
  return Number(data[0].last_block || 0);
}

async function setLastBlock(lastBlock) {
  const payload = {
    contract_address: CONTRACT_ADDRESS,
    last_block: Number(lastBlock),
    updated_at: new Date().toISOString()
  };
  // 用 upsert 需要表有 pk(contract_address)，我们建表时就是 pk
  const { error } = await supabase.from("chain_sync_state").upsert(payload);
  if (error) throw error;
}

// ===== Extract matchId from receipt (A: 实时写库) =====
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

// ===== MATCH LOOP (A) =====
let matchBusy = false;
async function tickMatch() {
  if (matchBusy) return;
  matchBusy = true;

  try {
    // RPC health check
    await provider.getBlockNumber();

    const pair = await claimTwoPlayers();
    if (!pair) {
      // console.log("[match] <2 queued");
      return;
    }

    const { a, b } = pair;
    console.log("[match] got pair:", a, b);

    // 可选：防止 DB 里 queued 但链上已退款/不在队列
    const [aq, bq] = await Promise.all([contract.inQueue(a), contract.inQueue(b)]);
    if (!aq) {
      console.log("[match] a not inQueue onchain -> set cancelled in DB:", a);
      await setQueueStatus(a, "cancelled");
      // 把 b 放回 queued（避免被锁在 locking）
      await setQueueStatus(b, "queued");
      return;
    }
    if (!bq) {
      console.log("[match] b not inQueue onchain -> set cancelled in DB:", b);
      await setQueueStatus(b, "cancelled");
      await setQueueStatus(a, "queued");
      return;
    }

    // 1) 先上链：只有失败才 rollback
    let receipt;
    try {
      const tx = await contract.lockMatch(a, b);
      console.log("[match] lockMatch tx:", tx.hash);
      receipt = await tx.wait();
    } catch (e) {
      console.error("[match] lockMatch failed:", e?.shortMessage || e?.message || e);
      await rollbackQueueToQueued(a, b);
      console.log("[match] rolled back queue -> queued");
      return;
    }

    const locked = extractMatchLocked(receipt);
    if (!locked) {
      // 极少数：tx 成功但我们没解析到事件（不太可能），当作写库失败处理，不回滚
      console.error("[match] tx succeeded but no MatchLocked event parsed; will rely on event scan");
      return;
    }

    console.log("[match] matchId =", locked.matchId);

    // 2) 上链成功后：写库失败也绝对不 rollback，只能依赖 B 补写
    try {
      await upsertMatchFromChain(locked.matchId, locked.a, locked.b, "locked");
      await markMatched(locked.a, locked.b);
      console.log("[match] ✅ wrote matches + marked matched");
    } catch (dbErr) {
      console.error("[match] DB write failed AFTER lockMatch (no rollback):", dbErr?.message || dbErr);
    }
  } catch (e) {
    console.error("[match] tick error:", e?.message || e);
  } finally {
    matchBusy = false;
  }
}

// ===== SYNC LOOP (B): 1) 扫 MatchLocked 事件补写 matches & queue 2) 同步 matches 状态 3) 纠偏 queue 状态 =====
let syncBusy = false;

async function scanMatchLockedEvents() {
  const latest = await provider.getBlockNumber();

  let last = await getLastBlock();
  if (!last || last <= 0) {
    // 首次启动，回扫一段区块以补写“之前漏写”的 matches
    last = Math.max(0, latest - EVENT_SCAN_BLOCKS);
    await setLastBlock(last);
  }

  const fromBlock = last + 1;
  if (fromBlock > latest) return;

  const filter = contract.filters.MatchLocked();
  const logs = await contract.queryFilter(filter, fromBlock, latest);

  if (logs.length > 0) {
    console.log("[sync] MatchLocked logs:", logs.length, "range:", fromBlock, "-", latest);
  }

  for (const ev of logs) {
    const matchId = Number(ev.args.matchId);
    const a = lc(ev.args.a);
    const b = lc(ev.args.b);

    try {
      await upsertMatchFromChain(matchId, a, b, "locked");
      // 只要链上锁过，就把两人标成 matched（链上 inMatch 应为 true）
      await markMatched(a, b);
    } catch (e) {
      console.error("[sync] event upsert failed:", matchId, e?.message || e);
    }
  }

  await setLastBlock(latest);
}

async function syncMatchStatuses() {
  const { data, error } = await supabase
    .from("matches")
    .select("chain_match_id,status")
    .eq("contract_address", CONTRACT_ADDRESS)
    .in("status", ["locked", "disputed"])
    .order("created_at", { ascending: true })
    .limit(SYNC_BATCH);

  if (error) throw error;
  if (!data || data.length === 0) return;

  for (const row of data) {
    const mid = Number(row.chain_match_id);
    const res = await contract.getMatch(mid);
    const st = Number(res[2]); // 1 locked,2 disputed,3 settled,4 cancelled

    if (st === 2 && row.status !== "disputed") {
      console.log("[sync] match", mid, "-> disputed");
      await updateMatchStatus(mid, "disputed");
    } else if (st === 3 && row.status !== "settled") {
      console.log("[sync] match", mid, "-> settled");
      await updateMatchStatus(mid, "settled");
    } else if (st === 4 && row.status !== "cancelled") {
      console.log("[sync] match", mid, "-> cancelled");
      await updateMatchStatus(mid, "cancelled");
    }
  }
}

async function fixQueueInconsistency() {
  // 修复你这次遇到的：DB=matched 但链上 inMatch=false / inQueue=true
  const { data, error } = await supabase
    .from("queue")
    .select("wallet,status")
    .in("status", ["matched", "locking"])
    .order("updated_at", { ascending: true })
    .limit(QUEUE_FIX_BATCH);

  if (error) throw error;
  if (!data || data.length === 0) return;

  for (const row of data) {
    const w = lc(row.wallet);
    const [im, iq] = await Promise.all([contract.inMatch(w), contract.inQueue(w)]);

    // 以链上为准纠偏
    let want;
    if (im) want = "matched";
    else if (iq) want = "queued";
    else want = "cancelled";

    if (row.status !== want) {
      console.log("[sync] fix queue", w, row.status, "->", want, `(onchain inMatch=${im} inQueue=${iq})`);
      await setQueueStatus(w, want);
    }
  }
}

async function tickSync() {
  if (syncBusy) return;
  syncBusy = true;

  try {
    await scanMatchLockedEvents(); // B-1: 补写 matches/queue
    await syncMatchStatuses();     // B-2: 同步 disputed/settled/cancelled
    await fixQueueInconsistency(); // B-3: 纠偏 queue
  } catch (e) {
    console.error("[sync] tick error:", e?.message || e);
  } finally {
    syncBusy = false;
  }
}

// ===== dummy http server for Render =====
const PORT = process.env.PORT || 10000;
http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("matchmaker running\n");
}).listen(PORT, () => {
  console.log("HTTP server listening on", PORT);
});

// ===== boot =====
console.log("Starting service...");
console.log("CONTRACT_ADDRESS:", CONTRACT_ADDRESS);
console.log("MATCH_INTERVAL_MS:", MATCH_INTERVAL_MS, "SYNC_INTERVAL_MS:", SYNC_INTERVAL_MS);

setInterval(tickMatch, MATCH_INTERVAL_MS);
setInterval(tickSync, SYNC_INTERVAL_MS);

// 立即执行一次
tickMatch();
tickSync();
