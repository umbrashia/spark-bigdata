# spark-bigdata

A **TypeScript-only** PySpark-style Spark API for Node.js, powered by `node4j` and JVM passthrough.

## Install

```bash
npm install spark-bigdata node4j
```

## Usage (TypeScript)

```ts
import { SparkSession } from 'spark-bigdata';
import node4j from 'node4j';

async function main(): Promise<void> {
  const spark = await SparkSession
    .builder(node4j)
    .appName('node-pyspark-parity')
    .master('local[*]')
    .config('spark.sql.shuffle.partitions', 4)
    .getOrCreate();

  const df = await spark.range(0, 100);
  await df.show(5);

  // Full parity fallback for any Spark JVM method:
  await spark.invoke('catalog');

  await spark.stop();
}

void main();
```



## Spark SQL & DataFrames (expanded)

```ts
const orders = await spark.read
  .format('parquet')
  .option('mergeSchema', true)
  .load('/data/orders');

const users = await spark.read.table('warehouse.users');
const amountCol = await spark.functions.col('amount');

const joined = await orders
  .join(users, 'user_id', 'left')
  .then((df) => df.withColumn('amount_bucket', amountCol));

await joined.createOrReplaceTempView('orders_enriched');
const top = await spark.sql('SELECT user_id, COUNT(*) c FROM orders_enriched GROUP BY user_id ORDER BY c DESC LIMIT 10');

await top.write
  .mode('overwrite')
  .format('json')
  .saveAsTable('analytics.top_users');
```

Added wrappers include:
- DataFrame transforms/actions: `join`, `withColumn`, `drop`, `dropDuplicates`, `orderBy`, `sort`, `limit`, `distinct`, `union`, `unionByName`, `repartition`, `coalesce`, `cache`, `persist`, `unpersist`, `count`, `first`, `head`, `take`, `toJSON`, `toPandas`.
- Batch reader/writer APIs: `spark.read` (`format`, `option(s)`, `load`, `json`, `csv`, `parquet`, `table`) and `df.write` (`format`, `mode`, `option(s)`, `partitionBy`, `bucketBy`, `sortBy`, `save`, `saveAsTable`, `insertInto`, `json`, `csv`, `parquet`).
- SQL helpers: `spark.functions` (`col`, `lit`, `when`, `invoke`), `spark.window` (`partitionBy`, `orderBy`), and `spark.catalog` (`listDatabases`, `listTables`, `listColumns`, cache controls).
- SparkSession parity helpers: `createDataFrame`, typed `udf`/`udtf` registration wrappers, `conf` runtime config wrapper, and `version` convenience accessor.
- View helpers: `createOrReplaceTempView` and `createGlobalTempView`.

## RDD API (expanded)

`RDD` now includes broader parity wrappers for common transformations and actions:
- Key/value ops: `reduceByKey`, `groupByKey`, `join`, `cogroup`.
- Partition ops: `mapPartitions`, `glom`, `repartition`, `coalesce`.
- Actions/output: `count`, `take`, `first`, `saveAsTextFile`.
- Persistence/checkpoint controls: `cache`, `persist`, `unpersist`, `checkpoint`, `localCheckpoint`, `isCheckpointed`, `getCheckpointFile`.

```ts
const pairs = (spark.invoke('sparkContext') as any).parallelize([['a', 1], ['a', 2], ['b', 1]]);
const rdd = pairs as any;

const reduced = rdd.reduceByKey((a: number, b: number) => a + b, 4);
reduced.cache();
reduced.checkpoint();
reduced.saveAsTextFile('/data/rdd-out');
```

## MLlib (expanded)

```ts
const tokenizer = await spark.ml.tokenizer();
const hashingTF = await spark.ml.hashingTF();
const idf = await spark.ml.idf();

const lr = await spark.ml.logisticRegression();
lr.setParams({ maxIter: 50, regParam: 0.01 });

const paramGrid = await spark.ml.paramGridBuilder();
paramGrid.addGrid('maxIter', [10, 50]).build();

const cv = await spark.ml.crossValidator();
cv.setEstimator(lr).setEvaluator(await spark.ml.multiclassClassificationEvaluator());

const model = await cv.fit(trainingDf);
const predictions = await model.transform(testDf);
```

Added MLlib wrappers include:
- Algorithms: `ALS`, `NaiveBayes`, `PCA`, `DecisionTreeRegressor` (plus previously wrapped classifiers/regressors/clustering).
- Tuning APIs: `CrossValidator`, `TrainValidationSplit`, `ParamGridBuilder`.
- Feature pipeline components: `Tokenizer`, `HashingTF`, `IDF`, `VectorIndexer`, `Imputer`, `VectorAssembler`, `StringIndexer`, `OneHotEncoder`, `StandardScaler`.
- Model persistence helpers: `save`, `write`, and MLlib loaders (`loadPipelineModel`, `loadCrossValidatorModel`, `loadTrainValidationSplitModel`).
- Params ergonomics: `setParams`, `getParam`, `getOrDefault`, `explainParams`, `extractParamMap`, `uid`.

For any MLlib API not wrapped yet, use generic construction:

```ts
const custom = await spark.ml.create('org.apache.spark.ml.feature.Bucketizer');
await custom.invoke('setInputCol', 'raw');
```


## Structured Streaming (expanded)

```ts
const input = await spark.readStream
  .kafka({ 'kafka.bootstrap.servers': 'localhost:9092', subscribe: 'events' })
  .load();

const query = await (await input.withWatermark('timestamp', '10 minutes')).writeStream
  .outputMode('append')
  .foreachBatch((batchDf: unknown, batchId: number) => {
    console.log('batch', batchId, batchDf);
  })
  .processingTime('10 seconds')
  .then((w) => w.start('/data/out'));

const progress = query.lastProgress();
const err = query.exception();
await query.stop();
```

Added streaming wrappers include:
- Advanced sinks/helpers: `foreachBatch`, `foreach`, `toTable` and `kafka`/`rate`/`textSocket` source helpers.
- Event-time convenience: `DataFrame.withWatermark(eventTimeColumn, delayThreshold)`.
- Trigger builder helpers: `processingTime`, `once`, `availableNow` (plus direct `trigger`).
- Rich query/query-manager controls: `lastProgress`, `recentProgress`, `exception`, `isActive`, `id`, `runId`, `name`, `explain`, `getByName`, `addListener`, `removeListener`.

Use `.invoke(...)` on wrappers for unwrapped streaming APIs.


## GraphFrames (implemented)

```ts
const vertices = await spark.range(0, 100);
const edges = await spark.range(0, 200);

const graph = await spark.graphframes.create(vertices, edges);
const triangles = await graph.find('(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)');

const bfs = await graph.bfs('id = 1', 'id = 42', 'relationship = "friend"', 5);
const ranked = await graph.pageRank({ resetProbability: 0.15, maxIter: 20 });
```

Use `.invoke(...)` on `GraphFrame` / `GraphFrames` for unwrapped APIs.


## Pandas API on Spark (implemented)

```ts
const psdf = await spark.pandasApi.range(0, 1000);
const csvDf = await spark.pandasApi.readCsv('/data/events.csv', { header: true });

const grouped = csvDf.groupBy('user_id');
const stats = await grouped.sum();

const sparkDf = stats.toSpark();
await sparkDf.show(10);
```

Use `.invoke(...)` on Pandas-on-Spark wrappers for unwrapped APIs.

## Parity strategy

- First-class wrappers in TypeScript for common PySpark-style classes (`SparkSession`, `DataFrame`, `GroupedData`, `RDD`, `MLlib`, `Structured Streaming`, `GraphFrames`, `Pandas API on Spark`).
- Generic dynamic JVM proxy passthrough for APIs not explicitly wrapped yet.
- `.invoke(methodName, ...args)` on wrappers to ensure newly added Spark APIs remain reachable.
