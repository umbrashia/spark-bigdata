import test from 'node:test';
import assert from 'node:assert/strict';

import { SparkSession } from '../src/pyspark';

function createNode4jRecorder() {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const staticMembers: Array<{ className: string; memberName: string }> = [];

  return {
    calls,
    staticMembers,
    client: {
      async newObject(): Promise<unknown> {
        return { __javaObjectId: 'unused' };
      },
      async getStaticMember(className: string, memberName: string): Promise<unknown> {
        staticMembers.push({ className, memberName });
        if (className === 'org.apache.spark.sql.SparkSession' && memberName === 'builder') {
          return { __javaObjectId: 'builder' };
        }

        return { __javaObjectId: `${className}.${memberName}` };
      },
      async call(_handle: unknown, methodName: string, ...args: unknown[]): Promise<unknown> {
        calls.push({ method: methodName, args });

        if (methodName === 'getOrCreate') {
          return { __javaObjectId: 'spark' };
        }

        if (methodName === 'readStream') {
          return { __javaObjectId: 'reader' };
        }

        if (methodName === 'load') {
          return { __javaObjectId: 'streaming-df' };
        }

        if (methodName === 'writeStream') {
          return { __javaObjectId: 'writer' };
        }

        if (methodName === 'start') {
          return { __javaObjectId: 'query' };
        }

        if (methodName === 'streams') {
          return { __javaObjectId: 'query-manager' };
        }

        if (methodName === 'get') {
          return { __javaObjectId: 'query-from-manager' };
        }

        if (methodName === 'withWatermark') {
          return { __javaObjectId: 'watermarked-streaming-df' };
        }

        if (methodName === 'apply') {
          return { __javaObjectId: 'trigger-instance' };
        }

        return { ok: true };
      },
    },
  };
}

test('DataStreamReader/DataStreamWriter advanced streaming controls are bridged', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const input = await spark.readStream
    .kafka({ 'kafka.bootstrap.servers': 'localhost:9092', subscribe: 'events' })
    .load();

  const query = await (await input.withWatermark('timestamp', '10 minutes')).writeStream
    .format('parquet')
    .outputMode('append')
    .queryName('events')
    .foreachBatch(() => undefined)
    .foreach({ open: () => true, process: () => undefined, close: () => undefined })
    .processingTime('10 seconds')
    .then((writer) => writer.start('/output'));

  query.lastProgress();
  query.recentProgress();
  query.exception();
  query.status();
  query.isActive();
  query.id();
  query.runId();
  query.name();
  query.explain(true);
  await query.awaitTermination(5000);

  assert.ok(recorder.calls.some((entry) => entry.method === 'readStream'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'format' && entry.args[0] === 'kafka'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'withWatermark'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'foreachBatch'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'foreach'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'trigger'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'start' && entry.args[0] === '/output'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'lastProgress'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'exception'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'awaitTermination' && entry.args[0] === 5000));
  assert.ok(recorder.staticMembers.some((entry) => entry.className === 'org.apache.spark.sql.streaming.Trigger' && entry.memberName === 'ProcessingTime'));
});

test('StreamingQueryManager rich controls delegate to JVM', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const query = await spark.streams.get('query-1');
  const queryByName = await spark.streams.getByName('my-query');
  assert.ok(query);
  assert.ok(queryByName);
  await query?.stop();

  spark.streams.addListener({});
  spark.streams.removeListener({});

  assert.ok(recorder.calls.some((entry) => entry.method === 'streams'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'get' && entry.args[0] === 'query-1'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'get' && entry.args[0] === 'my-query'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'addListener'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'removeListener'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'stop'));
});
