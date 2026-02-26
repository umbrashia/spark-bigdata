import test from 'node:test';
import assert from 'node:assert/strict';

import { SparkSession } from '../src/pyspark';

function createNode4jRecorder() {
  const calls: Array<{ method: string; args: unknown[] }> = [];

  return {
    calls,
    client: {
      async newObject(): Promise<unknown> {
        return { __javaObjectId: 'unused' };
      },
      async getStaticMember(className: string, memberName: string): Promise<unknown> {
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

        return { ok: true };
      },
    },
  };
}

test('DataStreamReader load + DataStreamWriter start are bridged', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const input = await spark.readStream
    .format('json')
    .option('maxFilesPerTrigger', 2)
    .load('/input');

  const query = await input.writeStream
    .format('parquet')
    .outputMode('append')
    .queryName('events')
    .start('/output');

  await query.awaitTermination(5000);

  assert.ok(recorder.calls.some((entry) => entry.method === 'readStream'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'writeStream'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'start' && entry.args[0] === '/output'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'awaitTermination' && entry.args[0] === 5000));
});

test('StreamingQueryManager get delegates to JVM', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const query = await spark.streams.get('query-1');
  assert.ok(query);
  await query?.stop();

  assert.ok(recorder.calls.some((entry) => entry.method === 'streams'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'get' && entry.args[0] === 'query-1'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'stop'));
});
