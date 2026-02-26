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

        if (className === 'pyspark.pandas' && memberName === '$MODULE$') {
          return { __javaObjectId: 'pandas-module' };
        }

        return { __javaObjectId: `${className}.${memberName}` };
      },
      async call(_handle: unknown, methodName: string, ...args: unknown[]): Promise<unknown> {
        calls.push({ method: methodName, args });

        if (methodName === 'getOrCreate') {
          return { __javaObjectId: 'spark' };
        }

        if (methodName === 'range' || methodName === 'read_csv' || methodName === 'DataFrame') {
          return { __javaObjectId: `psdf-${methodName}` };
        }

        if (methodName === 'to_spark') {
          return { __javaObjectId: 'spark-df' };
        }

        if (methodName === 'groupby' || methodName === 'sort_values') {
          return { __javaObjectId: methodName };
        }

        if (methodName === 'sum') {
          return { __javaObjectId: 'grouped-sum' };
        }

        return { ok: true };
      },
    },
  };
}

test('SparkSession pandasApi exposes range/read and conversion to spark DataFrame', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const psdf = await spark.pandasApi.range(0, 10);
  const fromCsv = await spark.pandasApi.readCsv('/tmp/file.csv', { header: true });
  const toSparkDf = fromCsv.toSpark();

  assert.ok(psdf);
  assert.ok(toSparkDf);
  assert.ok(recorder.calls.some((entry) => entry.method === 'range'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'read_csv'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'to_spark'));
});

test('Pandas-on-Spark groupBy operations route through JVM proxy', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const psdf = await spark.pandasApi.range(0, 100);
  const grouped = psdf.groupBy('id');
  const summed = await grouped.sum();

  assert.ok(summed);
  assert.ok(recorder.calls.some((entry) => entry.method === 'groupby'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'sum'));
});
