import test from 'node:test';
import assert from 'node:assert/strict';

import { SparkSessionBuilder, SparkSession, DataFrame, Column, RDD } from '../src/pyspark';
import { createJvmProxy, type JvmHandle } from '../src/proxy';

function handle(id: string): JvmHandle {
  return { __isJvmHandle: true, raw: { __javaObjectId: id } };
}

function createGatewayRecorder() {
  const calls: Array<[string, unknown, unknown?, unknown?]> = [];

  return {
    calls,
    gateway: {
      async getStatic(className: string, memberName: string): Promise<JvmHandle> {
        calls.push(['getStatic', className, memberName]);
        return handle(`${className}.${memberName}`);
      },
      async call(currentHandle: unknown, methodName: string, args: unknown[] = []): Promise<unknown> {
        calls.push(['call', currentHandle, methodName, args]);

        if (['getOrCreate', 'sql', 'read', 'write', 'catalog', 'table', 'join', 'union', 'unionByName', 'load', 'json', 'csv', 'parquet', 'col', 'lit', 'when', 'partitionBy', 'orderBy', 'createDataFrame', 'udf', 'udtf', 'conf'].includes(methodName)) {
          return handle(methodName);
        }

        if (methodName === 'apply') {
          return handle('applied');
        }

        return currentHandle;
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

test('Spark SQL batch reader/writer wrappers delegate and stay chainable', async () => {
  const { calls, gateway } = createGatewayRecorder();
  const session = new SparkSession(gateway as never, handle('spark'));
  const loaded = await session.read.format('parquet').option('header', true).load('/tmp/in');

  assert.ok(loaded instanceof DataFrame);

  loaded
    .write
    .format('json')
    .mode('overwrite')
    .option('compression', 'gzip')
    .save('/tmp/out');

  assert.ok(calls.find((entry) => entry[2] === 'read'));
  assert.ok(calls.find((entry) => entry[2] === 'format' && (entry[3] as unknown[])[0] === 'parquet'));
  assert.ok(calls.find((entry) => entry[2] === 'load'));
  assert.ok(calls.find((entry) => entry[2] === 'write'));
  assert.ok(calls.find((entry) => entry[2] === 'save'));
});

test('DataFrame parity methods and temp/global views delegate through gateway', async () => {
  const { calls, gateway } = createGatewayRecorder();
  const left = new DataFrame(gateway as never, handle('left-df'));
  const right = new DataFrame(gateway as never, handle('right-df'));
  const column = new Column(gateway as never, handle('col'));

  await left.join(right, 'id', 'inner');
  await left.union(right);
  await left.unionByName(right, true);
  await left.withColumn('flag', column);
  await left.createOrReplaceTempView('tmp_v');
  await left.createGlobalTempView('global_v');
  left.drop('a');
  left.dropDuplicates(['id']);
  left.orderBy('id');
  left.sort('ts');
  left.limit(10);
  left.distinct();
  left.repartition(2);
  left.coalesce(1);
  left.cache();
  left.persist();
  left.unpersist();
  left.count();
  left.first();
  left.head(3);
  left.take(2);
  left.toJSON();
  left.toPandas();

  assert.ok(calls.find((entry) => entry[2] === 'join'));
  assert.ok(calls.find((entry) => entry[2] === 'unionByName'));
  assert.ok(calls.find((entry) => entry[2] === 'withColumn'));
  assert.ok(calls.find((entry) => entry[2] === 'createOrReplaceTempView'));
  assert.ok(calls.find((entry) => entry[2] === 'createGlobalTempView'));
  assert.ok(calls.find((entry) => entry[2] === 'toPandas'));
});

test('column functions DSL and catalog APIs are available from SparkSession', async () => {
  const { calls, gateway } = createGatewayRecorder();
  const session = new SparkSession(gateway as never, handle('spark'));

  await session.functions.col('name');
  await session.functions.lit(1);
  const cond = new Column(gateway as never, handle('condition'));
  await session.functions.when(cond, 'x');
  await session.window.partitionBy('group');
  await session.window.orderBy('ts');

  session.catalog.listDatabases();
  session.catalog.listTables('default');
  session.catalog.cacheTable('t1');

  assert.ok(calls.find((entry) => entry[0] === 'getStatic' && entry[1] === 'org.apache.spark.sql.functions'));
  assert.ok(calls.find((entry) => entry[0] === 'getStatic' && entry[1] === 'org.apache.spark.sql.expressions.Window'));
  assert.ok(calls.find((entry) => entry[2] === 'catalog'));
  assert.ok(calls.find((entry) => entry[2] === 'cacheTable'));
});



test('SparkSession parity wrappers expose createDataFrame, udf/udtf, conf, and version', async () => {
  const { calls, gateway } = createGatewayRecorder();
  const session = new SparkSession(gateway as never, handle('spark'));

  const created = await session.createDataFrame([{ id: 1 }], { id: 'int' });
  assert.ok(created instanceof DataFrame);

  session.udf.register('my_udf', () => 1);
  session.udtf.register('my_udtf', 'com.example.MyTableFn');
  session.conf.set('spark.sql.shuffle.partitions', 8);
  session.conf.get('spark.sql.shuffle.partitions');
  session.conf.unset('spark.sql.shuffle.partitions');
  session.version;

  assert.ok(calls.find((entry) => entry[2] === 'createDataFrame'));
  assert.ok(calls.find((entry) => entry[2] === 'udf'));
  assert.ok(calls.find((entry) => entry[2] === 'udtf'));
  assert.ok(calls.find((entry) => entry[2] === 'conf'));
  assert.ok(calls.find((entry) => entry[2] === 'register' && (entry[3] as unknown[])[0] === 'my_udf'));
  assert.ok(calls.find((entry) => entry[2] === 'version'));
});



test('RDD parity wrappers delegate key/value, partition, action, and persistence operations', () => {
  const { calls, gateway } = createGatewayRecorder();
  const rdd = new RDD(gateway as never, handle('rdd-main'));
  const other = new RDD(gateway as never, handle('rdd-other'));

  rdd.map(() => 1);
  rdd.filter(() => true);
  rdd.flatMap(() => [1]);
  rdd.mapPartitions(() => []);
  rdd.glom();
  rdd.reduce((a: number, b: number) => a + b);
  rdd.reduceByKey((a: number, b: number) => a + b, 4);
  rdd.groupByKey(4);
  rdd.join(other, 3);
  rdd.cogroup(other, 2);
  rdd.repartition(10);
  rdd.coalesce(2, true);
  rdd.cache();
  rdd.persist();
  rdd.unpersist();
  rdd.checkpoint();
  rdd.localCheckpoint();
  rdd.isCheckpointed();
  rdd.getCheckpointFile();
  rdd.collect();
  rdd.count();
  rdd.first();
  rdd.take(5);
  rdd.saveAsTextFile('/tmp/out');

  assert.ok(calls.find((entry) => entry[2] === 'reduceByKey'));
  assert.ok(calls.find((entry) => entry[2] === 'groupByKey'));
  assert.ok(calls.find((entry) => entry[2] === 'join'));
  assert.ok(calls.find((entry) => entry[2] === 'cogroup'));
  assert.ok(calls.find((entry) => entry[2] === 'mapPartitions'));
  assert.ok(calls.find((entry) => entry[2] === 'repartition'));
  assert.ok(calls.find((entry) => entry[2] === 'coalesce'));
  assert.ok(calls.find((entry) => entry[2] === 'checkpoint'));
  assert.ok(calls.find((entry) => entry[2] === 'count'));
  assert.ok(calls.find((entry) => entry[2] === 'saveAsTextFile'));
});

test('createJvmProxy forwards unknown methods for parity fallback', async () => {
  const gateway = {
    async call(currentHandle: unknown, methodName: string, args: unknown[]) {
      return { ok: true, currentHandle, methodName, args };
    },
  };

  const proxy = createJvmProxy(gateway, handle('x'));
  const value = (await proxy.someFutureSparkMethod('a', 42)) as {
    ok: boolean;
    methodName: string;
    args: unknown[];
  };

  assert.equal(value.ok, true);
  assert.equal(value.methodName, 'someFutureSparkMethod');
  assert.deepEqual(value.args, ['a', 42]);
});
