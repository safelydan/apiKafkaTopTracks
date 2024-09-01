import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "spotify-producer",
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

export async function connectKafkaProducer() {
  try {
    await producer.connect();
    console.log("Kafka Producer conectado");
  } catch (error) {
    console.error("Erro ao conectar Kafka Producer:", error.message);
    throw error;
  }
}

export async function sendTopTracksToKafka(tracks) {
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

    console.log("Top 10 tracks enviadas para o Kafka");
  } catch (error) {
    console.error("Erro ao enviar faixas para o Kafka:", error.message);
    throw error;
  }
}

export async function disconnectKafkaProducer() {
  try {
    await producer.disconnect();
    console.log("Kafka Producer disconectado");
  } catch (error) {
    console.error("Erro ao desconectar Kafka Producer:", error.message);
    throw error;
  }
}
