import express from "express";
import cors from "cors";
import { consumeTopTracks, getTopTracks } from "../src/controller/consumer.js";

const app = express();
app.use(cors());

app.get("/api/tracks", (req, res) => {
  res.json(getTopTracks());
});

app.use((err, req, res, next) => {
  console.error("Error:", err.message);
  res.status(500).send("Internal Server Error");
});

const port = 3002;

app.listen(port, () => {
  console.log(`Servidor rodando em http://localhost:${port}`);
});

(async () => {
  await consumeTopTracks();
})();
