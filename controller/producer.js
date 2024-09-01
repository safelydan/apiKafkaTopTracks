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

    // Verifique se a resposta foi bem-sucedida
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

    return response.data.tracks.slice(0, 10); // Retorna as 10 músicas mais populares
  } catch (error) {
    console.error("Error getting top tracks:", error.message);
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
    console.log("Top 10 tracks sent to Kafka");
  } catch (error) {
    console.error("Error sending tracks to Kafka:", error.message);
    throw error;
  }
}

(async () => {
  try {
    await producer.connect();

    // Obtenha o token de acesso do Spotify
    const token = await getSpotifyToken();

    const artistId = "0oSGxfWSnnOXhD2fKuz2Gy"; // Substitua com o ID do artista desejado
    const topTracks = await getTopTracks(artistId, token);

    await sendTopTracksToKafka(topTracks);
  } catch (error) {
    console.error("An error occurred:", error.message);
  } finally {
    await producer.disconnect();
  }
})();
