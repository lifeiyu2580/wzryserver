import express from "express";

const app = express();
app.use(express.json());

/** ========= CORS 配置：只放行你的前端域名 =========
 * 1) 把 FRONTEND_ORIGIN 改成你的前端域名
 *    - 本地开发: http://localhost:5173
 *    - 线上: https://xxx.vercel.app 或 https://你的域名
 */
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "http://localhost:5173";

function setCors(res, origin) {
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Methods", "POST,GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Allow-Credentials", "false");
  // 缓存预检结果（可选）
  res.setHeader("Access-Control-Max-Age", "86400");
}

// 处理所有 OPTIONS 预检请求
app.options("*", (req, res) => {
  const origin = req.headers.origin;
  if (origin && origin === FRONTEND_ORIGIN) {
    setCors(res, origin);
    return res.status(204).end();
  }
  // 不允许的来源
  return res.status(403).end();
});

// 给所有响应都带上 CORS（仅允许指定域名）
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && origin === FRONTEND_ORIGIN) {
    setCors(res, origin);
  }
  next();
});

/** ========= health：保活用 ========= */
app.get("/health", (req, res) => {
  res.status(200).send("ok");
});

/** ========= flap GraphQL 代理 ========= */
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

/** ========= Render 启动 + 自 ping ========= */
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
