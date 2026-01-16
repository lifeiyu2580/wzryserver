import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { ethers } from "ethers";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

const SUPABASE_URL = mustEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = mustEnv("SUPABASE_SERVICE_ROLE_KEY");
const RPC_URL = mustEnv("RPC_URL");
const CONTRACT_ADDRESS = mustEnv("CONTRACT_ADDRESS");
const OPERATOR_PRIVATE_KEY = mustEnv("OPERATOR_PRIVATE_KEY");

const MATCH_INTERVAL_MS = Number(process.env.MATCH_INTERVAL_MS || 3000); // 3s
const SYNC_INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS || 5000);   // 5s
const SYNC_BATCH = Number(process.env.SYNC_BATCH || 50);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const ABI = [
  "function lockMatch(address a, address b)",
  "event MatchLocked(uint256 indexed matchId, address indexed a, address indexed b)",
  "function getMatch(uint256 matchId) view returns (address a,address b,uint8 status,uint8 reportA,uint8 reportB,address winReporter,uint64 confirmDeadline,address disputedBy,bytes32 disputeReasonHash,address winner)"
];

const provider = new ethers.JsonRpcProvider(RPC_URL, undefined, {
  polling: true,
  staticNetwork: ethers.Network.from(56) // BSC mainnet
});
const wallet = new ethers.Wallet(OPERATOR_PRIVATE_KEY, provider);
const contract = new ethers.Contract(CONTRACT_ADDRESS, ABI, wallet);

// --- helpers ---
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function claimTwoPlayers() {
  const { data, error } = await supabase.rpc("claim_two_players");
  if (error) throw error;
  if (!data || data.length === 0) return null;
  return { a: data[0].a_wallet, b: data[0].b_wallet };
}

async function rollbackQueue(a, b) {
  await supabase
    .from("queue")
    .update({ status: "queued", updated_at: new Date().toISOString() })
    .in("wallet", [a, b]);
}

async function insertMatch(matchId, a, b) {
  const { error } = await supabase.from("matches").insert({
    chain_match_id: Number(matchId),
    a_wallet: a,
    b_wallet: b,
    status: "locked",
    // 强烈建议加这列：防止你以后换合约串局
    contract_address: CONTRACT_ADDRESS
  });
  if (error) throw error;
}

async function markMatched(a, b) {
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
    .eq("chain_match_id", matchId)
    .eq("contract_address", CONTRACT_ADDRESS);
}

// --- MATCH LOOP: try lock one match each tick ---
let matchBusy = false;
async function tickMatch() {
  if (matchBusy) return;
  matchBusy = true;

  try {
    const block = await provider.getBlockNumber(); // quick health check
    // console.log("[match] block:", block);

    const pair = await claimTwoPlayers();
    if (!pair) {
      // console.log("[match] <2 queued");
      return;
    }

    const { a, b } = pair;
    console.log("[match] got pair:", a, b);

    try {
      const tx = await contract.lockMatch(a, b);
      console.log("[match] lockMatch tx:", tx.hash);
      const receipt = await tx.wait();

      let matchId = null;
      for (const log of receipt.logs) {
        try {
          const parsed = contract.interface.parseLog(log);
          if (parsed?.name === "MatchLocked") {
            matchId = parsed.args.matchId.toString();
            break;
          }
        } catch {}
      }
      if (!matchId) throw new Error("No MatchLocked event");

      console.log("[match] matchId =", matchId);

      await insertMatch(matchId, a, b);
      await markMatched(a, b);

      console.log("[match] ✅ inserted & marked matched");
    } catch (e) {
      console.error("[match] lockMatch failed:", e?.shortMessage || e?.message || e);
      await rollbackQueue(a, b);
      console.log("[match] rolled back queue -> queued");
    }
  } catch (e) {
    console.error("[match] tick error:", e?.message || e);
  } finally {
    matchBusy = false;
  }
}

// --- SYNC LOOP: sync chain status back to DB ---
let syncBusy = false;
async function tickSync() {
  if (syncBusy) return;
  syncBusy = true;

  try {
    // 拉一批未结束的对局
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
  } catch (e) {
    console.error("[sync] tick error:", e?.message || e);
  } finally {
    syncBusy = false;
  }
}

// --- boot ---
console.log("Starting service...");
console.log("CONTRACT_ADDRESS:", CONTRACT_ADDRESS);
console.log("MATCH_INTERVAL_MS:", MATCH_INTERVAL_MS, "SYNC_INTERVAL_MS:", SYNC_INTERVAL_MS);

setInterval(tickMatch, MATCH_INTERVAL_MS);
setInterval(tickSync, SYNC_INTERVAL_MS);

// first run immediately
tickMatch();
tickSync();
// ===== keep Render happy (dummy http server) =====
import http from "http";

const PORT = process.env.PORT || 3000;

http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("matchmaker running\n");
}).listen(PORT, () => {
  console.log("HTTP server listening on", PORT);
});
