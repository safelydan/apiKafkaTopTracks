import { Kafka } from 'kafkajs';
import express from 'express';

const kafka = new Kafka({ clientId: 'spotify-consumer', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'spotify-group' });

const app = express();
app.use(express.static('public'));  // Serve arquivos estáticos
app.set('view engine', 'ejs');

let topTracks = [];

async function consumeTopTracks() {
    try {
        await consumer.connect();
        console.log('Consumer connected');
        await consumer.subscribe({ topic: 'spotify-top-tracks', fromBeginning: true });
        console.log('Subscribed to topic');

        await consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const track = JSON.parse(message.value.toString());
                    console.log('Received track:', track);
                    topTracks.push(track);
                } catch (error) {
                    console.error('Error parsing message:', error.message);
                }
            },
        });
    } catch (error) {
        console.error('Error consuming tracks:', error.message);
    }
}

app.get('/api/tracks', (req, res) => {
    res.json(topTracks);
});

app.use((err, req, res, next) => {
    console.error('Error:', err.message);
    res.status(500).send('Internal Server Error');
});

app.listen(3000, () => console.log('Server running on http://localhost:3000'));

(async () => {
    await consumeTopTracks();
})();
