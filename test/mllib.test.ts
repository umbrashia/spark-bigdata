import test from 'node:test';
import assert from 'node:assert/strict';

import { SparkSession } from '../src/pyspark';

function createNode4jRecorder() {
  const created: Array<{ className: string; args: unknown[] }> = [];
  const called: Array<{ method: string; args: unknown[] }> = [];

  return {
    created,
    called,
    client: {
      async newObject(className: string, ...args: unknown[]): Promise<unknown> {
        created.push({ className, args });
        return { __javaObjectId: className };
      },
      async getStaticMember(className: string, memberName: string): Promise<unknown> {
        if (className === 'org.apache.spark.sql.SparkSession' && memberName === 'builder') {
          return { __javaObjectId: 'builder' };
        }

        return { __javaObjectId: `${className}.${memberName}` };
      },
      async call(_handle: unknown, methodName: string, ...args: unknown[]): Promise<unknown> {
        called.push({ method: methodName, args });

        if (methodName === 'getOrCreate') {
          return { __javaObjectId: 'spark' };
        }

        if (methodName === 'range') {
          return { __javaObjectId: 'df' };
        }

        if (methodName === 'fit') {
          return { __javaObjectId: 'pipeline-model' };
        }

        if (methodName === 'transform') {
          return { __javaObjectId: 'transformed-df' };
        }

        return { ok: true };
      },
    },
  };
}

test('SparkSession exposes MLlib constructors for common ML algorithms', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  await spark.ml.logisticRegression();
  await spark.ml.randomForestClassifier();
  await spark.ml.linearRegression();
  await spark.ml.kMeans();

  const classNames = recorder.created.map((entry) => entry.className);
  assert.ok(classNames.includes('org.apache.spark.ml.classification.LogisticRegression'));
  assert.ok(classNames.includes('org.apache.spark.ml.classification.RandomForestClassifier'));
  assert.ok(classNames.includes('org.apache.spark.ml.regression.LinearRegression'));
  assert.ok(classNames.includes('org.apache.spark.ml.clustering.KMeans'));
});

test('Pipeline fit/transform flow delegates to JVM ML methods', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const pipeline = await spark.ml.pipeline();
  const inputDf = await spark.range(0, 10);

  const model = await pipeline.fit(inputDf);
  const output = await model.transform(inputDf);

  assert.ok(output);
  assert.ok(recorder.called.some((entry) => entry.method === 'fit'));
  assert.ok(recorder.called.some((entry) => entry.method === 'transform'));
});
