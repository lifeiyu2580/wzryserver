import express from "express";

const app = express();
app.use(express.json());

/**
 * CORS：只放行一个前端域名（最安全）
 * Render 环境变量：
 * - FRONTEND_ORIGIN = https://你的前端域名（或 http://localhost:5173）
 * - SELF_URL = https://你的服务名.onrender.com
 */
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "http://localhost:5173";

function setCors(res, origin) {
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "POST,GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Max-Age", "86400");
}

/**
 * ✅ 用中间件处理预检 OPTIONS（不要用 app.options("*")）
 */
app.use((req, res, next) => {
  const origin = req.headers.origin;

  // 只允许指定来源
  if (origin && origin === FRONTEND_ORIGIN) {
    setCors(res, origin);
  }

  // 预检请求直接返回 204
  if (req.method === "OPTIONS") {
    // 如果 origin 不匹配，也别给 CORS 头（浏览器会拦）
    return res.status(origin === FRONTEND_ORIGIN ? 204 : 403).end();
  }

  next();
});

/** health：保活 */
app.get("/health", (req, res) => {
  res.status(200).send("ok");
});

/** flap GraphQL 代理 */
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

/** Render 启动 + 自 ping */
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("API running on port", PORT);
  console.log("CORS allowed origin:", FRONTEND_ORIGIN);

  const SELF_URL = process.env.SELF_URL; // 例：https://xxx.onrender.com
  if (SELF_URL) {
    console.log("Self ping enabled:", SELF_URL);
    setInterval(() => {
      fetch(`${SELF_URL}/health`).catch(() => {});
    }, 5 * 60 * 1000);
  } else {
    console.log("SELF_URL not set, self ping disabled");
  }
});
