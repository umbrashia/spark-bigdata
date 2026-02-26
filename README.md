# spark-bigdata

A Node.js-first Spark API designed as a PySpark substitute, powered by `node4j` and JVM passthrough.

## Why this exists

PySpark is Python-native, while Node.js teams often need the same Spark capabilities in JavaScript.
This package implements a **PySpark-like API surface** and includes a **generic JVM passthrough** so every Spark method is still reachable even when a dedicated JS helper is not yet added.

## Install

```bash
npm install spark-bigdata node4j
```

## Quick start

```js
const { SparkSession } = require('spark-bigdata');
const node4j = require('node4j');

async function main() {
  const spark = await SparkSession
    .builder(node4j)
    .appName('node-pyspark-parity')
    .master('local[*]')
    .config('spark.sql.shuffle.partitions', 4)
    .getOrCreate();

  const df = await spark.range(0, 100);
  await df.show(5);

  // Fallback path for any Spark API not explicitly wrapped:
  await spark.invoke('catalog');

  await spark.stop();
}

main().catch(console.error);
```

## Parity model

- Explicit wrappers: `SparkSession`, `DataFrame`, `GroupedData`, `RDD`.
- Full passthrough fallback: call any JVM Spark method through `.invoke(methodName, ...args)`.
- Dynamic proxy support: unknown methods are forwarded at runtime so new Spark features remain accessible without waiting for JS wrapper releases.

## Notes

- You must provide a configured `node4j` client connected to a JVM that has Apache Spark on its classpath.
- Serialization of JS lambdas to JVM callables depends on your `node4j` runtime strategy.
