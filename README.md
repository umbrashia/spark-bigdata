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


## MLlib (implemented)

```ts
const lr = await spark.ml.logisticRegression();
lr.set('maxIter', 50).set('regParam', 0.01);

const assembler = await spark.ml.vectorAssembler();
assembler.set('inputCols', ['f1', 'f2']).set('outputCol', 'features');

const pipeline = await spark.ml.pipeline();
pipeline.setStages([assembler, lr]);

const model = await pipeline.fit(trainingDf);
const predictions = await model.transform(testDf);

const evaluator = await spark.ml.multiclassClassificationEvaluator();
const score = await evaluator.evaluate(predictions);
```

For any MLlib API not wrapped yet, use generic construction:

```ts
const custom = await spark.ml.create('org.apache.spark.ml.feature.Bucketizer');
await custom.invoke('setInputCol', 'raw');
```


## Structured Streaming (implemented)

```ts
const input = await spark.readStream
  .format('json')
  .option('maxFilesPerTrigger', 1)
  .load('/data/in');

const query = await input.writeStream
  .format('parquet')
  .outputMode('append')
  .queryName('events')
  .start('/data/out');

await query.processAllAvailable();
await query.stop();
```

Use `spark.streams` to access query-manager behavior (`get`, `awaitAnyTermination`, `resetTerminated`) and `.invoke(...)` for any unwrapped streaming API.

## Parity strategy

- First-class wrappers in TypeScript for common PySpark-style classes (`SparkSession`, `DataFrame`, `GroupedData`, `RDD`, `MLlib`, `Structured Streaming`).
- Generic dynamic JVM proxy passthrough for APIs not explicitly wrapped yet.
- `.invoke(methodName, ...args)` on wrappers to ensure newly added Spark APIs remain reachable.
