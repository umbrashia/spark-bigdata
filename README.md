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

## Parity strategy

- First-class wrappers in TypeScript for common PySpark-style classes (`SparkSession`, `DataFrame`, `GroupedData`, `RDD`).
- Generic dynamic JVM proxy passthrough for APIs not explicitly wrapped yet.
- `.invoke(methodName, ...args)` on wrappers to ensure newly added Spark APIs remain reachable.
