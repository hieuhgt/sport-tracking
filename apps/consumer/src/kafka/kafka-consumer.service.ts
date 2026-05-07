import {
  Injectable, Logger,
  OnApplicationBootstrap, OnApplicationShutdown,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Consumer } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { SportEvent } from '@sport-tracking/schemas';
import { StateService } from '../state/state.service';
import { EventsGateway } from '../events/events.gateway';

@Injectable()
export class KafkaConsumerService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private consumer!: Consumer;

  constructor(
    private config: ConfigService,
    private stateService: StateService,
    private eventsGateway: EventsGateway,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    const kafka = new Kafka({
      clientId: `sport-consumer-${process.env.HOSTNAME ?? Math.random().toString(36).slice(2)}`,
      brokers: (this.config.get<string>('KAFKA_BROKERS') ?? 'localhost:9092').split(','),
      retry: { initialRetryTime: 300, retries: 10 },
    });

    const registry = new SchemaRegistry({
      host: this.config.get<string>('SCHEMA_REGISTRY_URL') ?? 'http://localhost:8081',
    });

    this.consumer = kafka.consumer({
      groupId: this.config.get<string>('KAFKA_GROUP_ID') ?? 'sports-consumer-group',
      heartbeatInterval: 3000,
      sessionTimeout: 30000,
    });

    await this.consumer.connect();
    this.logger.log('Connected to Kafka');

    await this.consumer.subscribe({ topics: ['worldcup.events', 'nba.events'], fromBeginning: false });

    await this.consumer.run({
      partitionsConsumedConcurrently: 3,
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;
        try {
          const event = (await registry.decode(message.value)) as SportEvent;
          const state = await this.stateService.applyEvent(event);
          this.eventsGateway.broadcastUpdate(event.sport, state);
        } catch (err) {
          this.logger.error(`Decode error topic=${topic} partition=${partition}`, err);
        }
      },
    });
  }

  async onApplicationShutdown(): Promise<void> {
    await this.consumer?.disconnect();
    this.logger.log('Kafka consumer disconnected');
  }
}
