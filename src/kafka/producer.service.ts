import { Injectable, OnApplicationShutdown, OnModuleInit } from "@nestjs/common";
import { Kafka, Producer, ProducerRecord } from "kafkajs";
import { brokerAdd } from "./constants";

@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown{
    private readonly kafka = new Kafka({
        brokers: [brokerAdd]
    })
    private readonly producer: Producer = this.kafka.producer();

    async onModuleInit() {
        await this.producer.connect();
    }

    async onApplicationShutdown(signal?: string) {
        await this.producer.disconnect();
    }

    async produce(record: ProducerRecord) {
        await this.producer.send(record);
    }

    
}