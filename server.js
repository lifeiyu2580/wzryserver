import express from "express";

const app = express();
app.use(express.json());

// ===== 1. health：保活用 =====
app.get("/health", (req, res) => {
  res.status(200).send("ok");
});

// ===== 2. flap GraphQL 代理 =====
app.post("/api/coin", async (req, res) => {
  try {
    const { address } = req.body || {};
    if (!address) {
      return res.status(400).json({ error: "missing address" });
    }

    const payload = {
      query: `
        query Coin($address:String) {
          coin(address: $address) {
            posts {
              timestamp
              content
              tx
              profile {
                address
                name
                pfp
              }
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

// ===== 3. 启动 + 自 ping 保活 =====
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("API running on port", PORT);

  const SELF_URL = process.env.SELF_URL;
  if (SELF_URL) {
    console.log("Self ping enabled:", SELF_URL);

    setInterval(() => {
      fetch(`${SELF_URL}/health`)
        .then(() => {
          console.log("self ping ok");
        })
        .catch(() => {
          console.log("self ping failed");
        });
    }, 5 * 60 * 1000); // 每 5 分钟 ping 一次
  } else {
    console.log("SELF_URL not set, self ping disabled");
  }
});
