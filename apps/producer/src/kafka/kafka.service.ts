import { Injectable, OnApplicationBootstrap, OnApplicationShutdown, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Producer, CompressionTypes } from 'kafkajs';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { SportEvent, worldcupEventSchema, nbaEventSchema } from '@sport-tracking/schemas';

const TOPICS = [
  { topic: 'worldcup.events', numPartitions: 6, replicationFactor: 3 },
  { topic: 'nba.events',      numPartitions: 6, replicationFactor: 3 },
];

@Injectable()
export class KafkaService implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly logger = new Logger(KafkaService.name);
  private kafka!: Kafka;
  private producer!: Producer;
  private registry!: SchemaRegistry;
  private schemaIds: Record<string, number> = {};

  constructor(private config: ConfigService) {}

  async onApplicationBootstrap(): Promise<void> {
    this.kafka = new Kafka({
      clientId: 'sport-producer',
      brokers: (this.config.get<string>('KAFKA_BROKERS') ?? 'localhost:9092').split(','),
      retry: { initialRetryTime: 300, retries: 10 },
    });

    await this.ensureTopics();

    this.registry = new SchemaRegistry({
      host: this.config.get<string>('SCHEMA_REGISTRY_URL') ?? 'http://localhost:8081',
    });
    await this.initRegistry();

    this.producer = this.kafka.producer({ idempotent: true, maxInFlightRequests: 5 });
    await this.producer.connect();
    this.logger.log('Kafka producer connected');
  }

  async onApplicationShutdown(): Promise<void> {
    await this.producer?.disconnect();
    this.logger.log('Kafka producer disconnected');
  }

  async publishEvent(event: SportEvent): Promise<void> {
    const topic = event.sport === 'worldcup' ? 'worldcup.events' : 'nba.events';
    const encoded = await this.registry.encode(this.schemaIds[topic], event);
    await this.producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{ key: event.matchId, value: encoded, timestamp: String(event.timestamp) }],
    });
  }

  private async ensureTopics(): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const existing = await admin.listTopics();
      const toCreate = TOPICS.filter((t) => !existing.includes(t.topic));
      if (toCreate.length > 0) {
        await admin.createTopics({ topics: toCreate });
        toCreate.forEach((t) => this.logger.log(`Created topic "${t.topic}"`));
      } else {
        this.logger.log('Topics already exist');
      }
    } finally {
      await admin.disconnect();
    }
  }

  private async initRegistry(): Promise<void> {
    const [wcResult, nbaResult] = await Promise.all([
      this.registry.register(
        { type: SchemaType.AVRO, schema: JSON.stringify(worldcupEventSchema) },
        { subject: 'worldcup.events-value' },
      ),
      this.registry.register(
        { type: SchemaType.AVRO, schema: JSON.stringify(nbaEventSchema) },
        { subject: 'nba.events-value' },
      ),
    ]);
    this.schemaIds['worldcup.events'] = wcResult.id;
    this.schemaIds['nba.events']      = nbaResult.id;
    this.logger.log(`worldcup.events-value → schema id=${wcResult.id}`);
    this.logger.log(`nba.events-value      → schema id=${nbaResult.id}`);
  }
}
