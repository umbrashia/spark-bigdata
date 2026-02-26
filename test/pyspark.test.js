'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { SparkSessionBuilder } = require('../src/pyspark');
const { createJvmProxy } = require('../src/proxy');

function createGatewayRecorder() {
  const calls = [];

  return {
    calls,
    gateway: {
      async getStatic(className, memberName) {
        calls.push(['getStatic', className, memberName]);
        return { __isJvmHandle: true, raw: { __javaObjectId: 'builder' } };
      },
      async call(handle, methodName, args = []) {
        calls.push(['call', handle, methodName, args]);

        if (methodName === 'getOrCreate') {
          return { __isJvmHandle: true, raw: { __javaObjectId: 'spark' } };
        }

        if (methodName === 'sql') {
          return { __isJvmHandle: true, raw: { __javaObjectId: 'df' } };
        }

        return handle;
      },
    },
  };
}

test('SparkSessionBuilder configures and creates a SparkSession', async () => {
  const { calls, gateway } = createGatewayRecorder();
  const builder = new SparkSessionBuilder(gateway)
    .appName('node4j-pyspark')
    .master('local[*]')
    .config('spark.sql.shuffle.partitions', 8)
    .enableHiveSupport();

  const session = await builder.getOrCreate();
  await session.sql('SELECT 1');

  assert.equal(calls[0][0], 'getStatic');
  assert.ok(calls.find((entry) => entry[2] === 'config' && entry[3][0] === 'spark.app.name'));
  assert.ok(calls.find((entry) => entry[2] === 'config' && entry[3][0] === 'spark.master'));
  assert.ok(calls.find((entry) => entry[2] === 'enableHiveSupport'));
  assert.ok(calls.find((entry) => entry[2] === 'sql'));
});

test('createJvmProxy forwards unknown methods for parity fallback', async () => {
  const gateway = {
    async call(handle, methodName, args) {
      return { ok: true, handle, methodName, args };
    },
  };

  const proxy = createJvmProxy(gateway, { __isJvmHandle: true, raw: { __javaObjectId: 'x' } });
  const value = await proxy.someFutureSparkMethod('a', 42);

  assert.equal(value.ok, true);
  assert.equal(value.methodName, 'someFutureSparkMethod');
  assert.deepEqual(value.args, ['a', 42]);
});
