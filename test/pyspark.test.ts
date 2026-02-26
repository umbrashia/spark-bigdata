import test from 'node:test';
import assert from 'node:assert/strict';

import { SparkSessionBuilder } from '../src/pyspark';
import { createJvmProxy, type JvmHandle } from '../src/proxy';

function createGatewayRecorder() {
  const calls: Array<[string, unknown, unknown?, unknown?]> = [];

  return {
    calls,
    gateway: {
      async getStatic(className: string, memberName: string): Promise<JvmHandle> {
        calls.push(['getStatic', className, memberName]);
        return { __isJvmHandle: true, raw: { __javaObjectId: 'builder' } };
      },
      async call(handle: unknown, methodName: string, args: unknown[] = []): Promise<unknown> {
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
  const builder = new SparkSessionBuilder(gateway as never)
    .appName('node4j-pyspark')
    .master('local[*]')
    .config('spark.sql.shuffle.partitions', 8)
    .enableHiveSupport();

  const session = await builder.getOrCreate();
  await session.sql('SELECT 1');

  assert.equal(calls[0][0], 'getStatic');
  assert.ok(calls.find((entry) => entry[2] === 'config' && (entry[3] as string[])[0] === 'spark.app.name'));
  assert.ok(calls.find((entry) => entry[2] === 'config' && (entry[3] as string[])[0] === 'spark.master'));
  assert.ok(calls.find((entry) => entry[2] === 'enableHiveSupport'));
  assert.ok(calls.find((entry) => entry[2] === 'sql'));
});

test('createJvmProxy forwards unknown methods for parity fallback', async () => {
  const gateway = {
    async call(handle: unknown, methodName: string, args: unknown[]) {
      return { ok: true, handle, methodName, args };
    },
  };

  const proxy = createJvmProxy(gateway, { __isJvmHandle: true, raw: { __javaObjectId: 'x' } });
  const value = (await proxy.someFutureSparkMethod('a', 42)) as {
    ok: boolean;
    methodName: string;
    args: unknown[];
  };

  assert.equal(value.ok, true);
  assert.equal(value.methodName, 'someFutureSparkMethod');
  assert.deepEqual(value.args, ['a', 42]);
});
