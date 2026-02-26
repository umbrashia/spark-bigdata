export { SparkSession, SparkSessionBuilder, DataFrame, GroupedData, RDD } from './src/pyspark';
export { Node4jGateway } from './src/gateway';
export { createJvmProxy } from './src/proxy';

export type { Node4jClient } from './src/gateway';
export type { JvmHandle, JvmProxy } from './src/proxy';
