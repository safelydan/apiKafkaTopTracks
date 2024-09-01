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
    console.log("Consumer connected");
    await consumer.subscribe({
      topic: "spotify-top-tracks",
      fromBeginning: true,
    });
    console.log("Subscribed to topic");

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const track = JSON.parse(message.value.toString());
          console.log("Received track:", track);
          topTracks.push(track);
        } catch (error) {
          console.error("Error parsing message:", error.message);
        }
      },
    });
  } catch (error) {
    console.error("Error consuming tracks:", error.message);
  }
}

export function getTopTracks() {
  return topTracks;
}
