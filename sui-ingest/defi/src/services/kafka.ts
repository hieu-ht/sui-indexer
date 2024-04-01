import { Kafka, CompressionTypes, CompressionCodecs } from "kafkajs";
import SnappyCodec from "kafkajs-snappy";

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

export const kafkaClient = new Kafka({
  clientId: "nimbus-sui-indexer",
  brokers: String(
    process.env.KAFKA_BROKERS || "redpanda-cluster.getnimbus.xyz:19092"
  ).split(","),
});

export const producer = kafkaClient.producer();

export const initKafka = async () => {
  try {
    await producer.connect();
  } catch (err) {
    console.log(err);
    throw err;
  }
};
