import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { Kafka, Consumer, ConsumerSubscribeTopics, ConsumerRunConfig } from 'kafkajs';
import { brokerAdd } from './constants';

@Injectable()
export class ConsumerService implements OnApplicationShutdown{
  private readonly kafka = new Kafka({
    brokers: [brokerAdd],
  });

  private readonly consumers: Consumer[] = [];

  async consume(topic: ConsumerSubscribeTopics, config: ConsumerRunConfig) {
    const consumer = this.kafka.consumer({groupId: 'nestjs-kafka'});
    await consumer.connect();
    await consumer.subscribe(topic);
    await consumer.run(config);
    this.consumers.push(consumer);
  }

  async onApplicationShutdown(signal?: string) {
      for(const consumer of this.consumers) {
        await consumer.disconnect();
      }
  }
}
