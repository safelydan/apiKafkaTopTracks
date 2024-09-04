import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer({
  retry: {
    retries: 5, // número de tentativas
    initialRetryTime: 300, // tempo inicial de espera entre tentativas
  },
});

export async function connectKafkaProducer() {
  try {
    await producer.connect();
    console.log("Kafka Producer de email conectado");
  } catch (error) {
    console.error("Erro ao conectar Kafka Producer de email:", error.message);
    throw error;
  }
}

export async function sendNewReleasesToKafka(newReleases) {
  try {
    await connectKafkaProducer();
    await producer.send({
      topic: "new-artist-releases",
      messages: [{ value: JSON.stringify(newReleases) }],
    });
    console.log("Novos lançamentos enviados para o Kafka");
  } catch (error) {
    console.error("Erro ao enviar lançamentos para o Kafka:", error.message);
    throw error;
  }
}

export async function disconnectKafkaProducer() {
  try {
    await producer.disconnect();
    console.log("Kafka Producer de email desconectado");
  } catch (error) {
    console.error("Erro ao desconectar Kafka Producer de email:", error.message);
    throw error;
  }
}
