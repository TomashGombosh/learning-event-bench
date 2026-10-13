import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createBroker } from '@adapters/broker-factory';
import { AppModule } from './app.module';
import { loadGatewayConfig } from './config';

async function bootstrap(): Promise<void> {
  const config = loadGatewayConfig();
  const broker = await createBroker(config.BROKER, {
    redisUrl: config.REDIS_URL,
    kafkaBrokers: config.KAFKA_BROKERS.split(','),
    kafkaPartitions: config.KAFKA_PARTITIONS,
    streamMaxLen: config.STREAM_MAXLEN,
  });
  const app = await NestFactory.create(AppModule.register(broker), { logger: ['error', 'warn', 'log'] });
  app.useWebSocketAdapter(new IoAdapter(app));
  app.enableShutdownHooks();
  await app.listen(config.PORT);
  console.log(`gateway listening on ${config.PORT} with broker=${broker.bus.name}`);
  const shutdown = async (): Promise<void> => {
    await app.close();
    await broker.bus.close();
    process.exit(0);
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

void bootstrap();
