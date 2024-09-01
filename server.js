import express from "express";
import cors from "cors";
import { consumeTopTracks, getTopTracks } from "./controller/consumer.js";

const app = express();
app.use(cors());

app.get("/api/tracks", (req, res) => {
  res.json(getTopTracks());
});

app.use((err, req, res, next) => {
  console.error("Error:", err.message);
  res.status(500).send("Internal Server Error");
});

app.listen(3002, () =>
  console.log("Servidor rodando em http://localhost:3002")
);

(async () => {
  await consumeTopTracks();
})();
