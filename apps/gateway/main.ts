import 'reflect-metadata';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createBroker } from '@adapters/broker-factory';
import { createOtBackend, createOtDatabase } from '@adapters/sharedb/backend';
import { createShareDbPubSub } from '@adapters/sharedb/pubsub-factory';
import { AppModule } from './app.module';
import { GatewayConfig, loadGatewayConfig } from './config';
import { attachOtServer, mountOtState } from './ot-server';

@Module({})
class OtModule {}

async function bootstrap(): Promise<void> {
  const config = loadGatewayConfig();
  if (config.SCHEME === 'sharedb') {
    await bootShareDb(config);
    return;
  }
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
  console.log(`gateway listening on ${config.PORT} scheme=yjs broker=${broker.bus.name}`);
  const shutdown = async (): Promise<void> => {
    await app.close();
    await broker.bus.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

async function bootShareDb(config: GatewayConfig): Promise<void> {
  const db = createOtDatabase(config.OT_DB, config.DATABASE_URL);
  const bundle = await createShareDbPubSub(config.BROKER, {
    redisUrl: config.REDIS_URL,
    kafkaBrokers: config.KAFKA_BROKERS.split(','),
    kafkaPartitions: config.KAFKA_PARTITIONS,
    streamMaxLen: config.STREAM_MAXLEN,
  });
  const backend = await createOtBackend(bundle.pubsub, db);
  const app = await NestFactory.create(OtModule, { logger: ['error', 'warn', 'log'] });
  mountOtState(app.getHttpAdapter().getInstance(), db);
  app.enableShutdownHooks();
  await app.listen(config.PORT);
  const sockets = attachOtServer(app.getHttpServer(), backend);
  console.log(`gateway listening on ${config.PORT} scheme=sharedb broker=${config.BROKER} otDb=${config.OT_DB}`);
  const shutdown = async (): Promise<void> => {
    await sockets.close();
    await bundle.close();
    if (typeof db.close === 'function') {
      await new Promise<void>((resolve) => db.close(() => resolve()));
    }
    await app.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

void bootstrap();
