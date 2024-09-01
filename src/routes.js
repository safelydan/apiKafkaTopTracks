import { consumeTopTracks, getTopTracks } from "../src/controller/consumer.js";
import express from 'express'
const router = express.Router()

router.get("/tracks", (req, res) => {
  res.json(getTopTracks());
});

(async () => {
  await consumeTopTracks();
})();

export default router;