import { Kafka } from "kafkajs";
import axios from "axios";

// Configurações do Kafka
const kafka = new Kafka({
  clientId: "spotify-producer",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

// Credenciais do Spotify
const clientId = "ec15723b27e64972ac2bd688d71b2380";
const clientSecret = "f9170e1cc81146e5853096160963278d";

async function getSpotifyToken() {
  try {
    const response = await fetch("https://accounts.spotify.com/api/token", {
      method: "POST",
      body: new URLSearchParams({ grant_type: "client_credentials" }),
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization:
          "Basic " +
          Buffer.from(clientId + ":" + clientSecret).toString("base64"),
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data.access_token;
  } catch (error) {
    console.error("Error getting Spotify token:", error.message);
    throw error;
  }
}

async function getTopTracks(artistId, token) {
  try {
    const response = await axios.get(
      `https://api.spotify.com/v1/artists/${artistId}/top-tracks`,
      {
        params: { market: "US" },
        headers: { Authorization: `Bearer ${token}` },
      }
    );

    return response.data.tracks.slice(0, 10);
  } catch (error) {
    console.error("Erro ao tentar pegar as tracks:", error.message);
    throw error;
  }
}

async function sendTopTracksToKafka(tracks) {
  try {
    const messages = tracks.map((track) => ({
      value: JSON.stringify({
        name: track.name,
        album: track.album.name,
        preview_url: track.preview_url,
        image_url: track.album.images[0]?.url,
      }),
    }));

    await producer.send({
      topic: "spotify-top-tracks",
      messages: messages,
    });

    console.log(messages);
    console.log("Top 10 faixas enviadas para o Kafka");
  } catch (error) {
    console.error("Erro enviar faixas para o Kafka:", error.message);
    throw error;
  }
}

(async () => {
  try {
    await producer.connect();
    const token = await getSpotifyToken();

    const artistId = "4STHEaNw4mPZ2tzheohgXB"; // Substitua com o ID do artista desejado
    const topTracks = await getTopTracks(artistId, token);

    await sendTopTracksToKafka(topTracks);
  } catch (error) {
    console.error("Um erro aconteceu:", error.message);
  } finally {
    await producer.disconnect();
  }
})();
