import express from "express";

const app = express();
app.use(express.json());

/**
 * 允许的 Origin 列表（逗号分隔）
 * 本地开发必须包含 http://localhost:5173
 */
const FRONTEND_ORIGINS = (process.env.FRONTEND_ORIGINS || "http://localhost:5173")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

function isAllowedOrigin(origin) {
  return FRONTEND_ORIGINS.includes(origin);
}

function setCors(res, origin) {
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "POST,GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Max-Age", "86400");
}

/**
 * ✅ 统一处理 CORS + OPTIONS
 * - OPTIONS 永远 204（不再 403）
 * - 只有白名单 Origin 才会得到 Allow-Origin
 */
app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (origin && isAllowedOrigin(origin)) {
    setCors(res, origin);
  }

  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }

  next();
});

app.get("/health", (req, res) => res.status(200).send("ok"));

app.post("/api/coin", async (req, res) => {
  try {
    const { address } = req.body || {};
    if (!address) return res.status(400).json({ error: "missing address" });

    const payload = {
      query: `
        query Coin($address:String) {
          coin(address: $address) {
            posts {
              timestamp
              content
              tx
              profile { address name pfp }
            }
          }
        }
      `,
      variables: { address }
    };

    const r = await fetch("https://0pi75kmgw9.execute-api.eu-west-3.amazonaws.com/v1", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const json = await r.json();
    res.status(r.status).json(json);
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("API running on port", PORT);
  console.log("CORS allowed origins:", FRONTEND_ORIGINS);

  const SELF_URL = process.env.SELF_URL;
  if (SELF_URL) {
    setInterval(() => {
      fetch(`${SELF_URL}/health`).catch(() => {});
    }, 5 * 60 * 1000);
  }
});
