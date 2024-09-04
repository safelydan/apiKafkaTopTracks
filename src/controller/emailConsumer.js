import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "email-group" });

export async function consumeNewReleases() {
  try {
    await consumer.connect();
    console.log("Email consumer conectado");
    await consumer.subscribe({ topic: "new-artist-releases", fromBeginning: false });
    console.log("Email inscrito no tópico");

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const newReleases = JSON.parse(message.value.toString());
          console.log("Novos lançamentos recebidos:", newReleases);

          await sendEmailForNewReleases(newReleases);
          console.log("E-mail enviado com sucesso para novos lançamentos");
          return newReleases
        } catch (error) {
          console.error("Erro ao processar mensagem:", error.message);
        }
      },
    });
  } catch (error) {
    console.error("Erro ao consumir lançamentos:", error.message);
  }
}

export async function disconnectKafkaConsumer() {
  try {
    await consumer.disconnect();
    console.log("Kafka Consumer desconectado");
  } catch (error) {
    console.error("Erro ao desconectar Kafka Consumer:", error.message);
    throw error;
  }
}
