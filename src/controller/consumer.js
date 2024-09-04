import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "spotify-consumer",
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "spotify-group" });

let topTracks = [];

export async function consumeTopTracks() {
  try {
    await consumer.connect();
    console.log("Track consumer connected");
    await consumer.subscribe({
      topic: "spotify-top-tracks",
      fromBeginning: true,
    });
    console.log("Track subscribed to topic");

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const track = JSON.parse(message.value.toString());
          console.log("Faixa recebida:", track);
          topTracks.push(track);

          if (topTracks.length > 10) {
            topTracks.shift();
          }
        } catch (error) {
          console.error("Erro ao converter mensagem :", error.message);
        }
      },
    });
  } catch (error) {
    console.error("Erro ao consumir faixas:", error.message);
  }
}

export function getTopTracks() {
  return topTracks;
}
