import { createJvmProxy, type JvmHandle } from './proxy';
import type { Node4jGateway } from './gateway';
import { DataFrame, SparkSession } from './pyspark';

export class PandasOnSparkDataFrame {
  private readonly gateway: Node4jGateway;
  private readonly handle: JvmHandle;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  head(n = 5): unknown {
    return this.proxy.head(n);
  }

  describe(): Promise<PandasOnSparkDataFrame> {
    return this.wrapDataFramePromise(this.proxy.describe());
  }

  sortValues(by: string | string[], ascending: boolean | boolean[] = true): Promise<PandasOnSparkDataFrame> {
    return this.wrapDataFramePromise(this.proxy.sort_values(by, ascending));
  }

  groupBy(...keys: string[]): PandasOnSparkGroupBy {
    const groupedProxy = this.proxy.groupby(...keys) as { __handle: JvmHandle };
    return new PandasOnSparkGroupBy(this.gateway, groupedProxy.__handle);
  }

  toSpark(): DataFrame {
    const sparkDfProxy = this.proxy.to_spark() as { __handle: JvmHandle };
    return new DataFrame(this.gateway, sparkDfProxy.__handle);
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }

  private async wrapDataFramePromise(value: unknown): Promise<PandasOnSparkDataFrame> {
    const result = await value;
    const proxyValue = result as { __handle: JvmHandle };
    return new PandasOnSparkDataFrame(this.gateway, proxyValue.__handle);
  }
}

export class PandasOnSparkSeries {
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class PandasOnSparkGroupBy {
  private readonly gateway: Node4jGateway;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.proxy = createJvmProxy(gateway, handle);
  }

  sum(): Promise<PandasOnSparkDataFrame> {
    return this.wrapDataFramePromise(this.proxy.sum());
  }

  mean(): Promise<PandasOnSparkDataFrame> {
    return this.wrapDataFramePromise(this.proxy.mean());
  }

  count(): Promise<PandasOnSparkDataFrame> {
    return this.wrapDataFramePromise(this.proxy.count());
  }

  agg(expr: unknown): Promise<PandasOnSparkDataFrame> {
    return this.wrapDataFramePromise(this.proxy.agg(expr));
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }

  private async wrapDataFramePromise(value: unknown): Promise<PandasOnSparkDataFrame> {
    const result = await value;
    const proxyValue = result as { __handle: JvmHandle };
    return new PandasOnSparkDataFrame(this.gateway, proxyValue.__handle);
  }
}

export class PandasOnSpark {
  private readonly gateway: Node4jGateway;
  private readonly spark: SparkSession;

  constructor(gateway: Node4jGateway, spark: SparkSession) {
    this.gateway = gateway;
    this.spark = spark;
  }

  get dataFrameClass(): Promise<JvmHandle> {
    return this.gateway.getStatic('pyspark.pandas', 'DataFrame');
  }

  async range(start: number, end?: number, step = 1, numPartitions?: number): Promise<PandasOnSparkDataFrame> {
    const args: unknown[] = [start];

    if (typeof end === 'number') {
      args.push(end);
    }

    args.push(step);

    if (typeof numPartitions === 'number') {
      args.push(numPartitions);
    }

    const result = await this.invoke('range', ...args);
    const proxyValue = result as { __handle: JvmHandle };
    return new PandasOnSparkDataFrame(this.gateway, proxyValue.__handle);
  }

  async readCsv(path: string, options: Record<string, unknown> = {}): Promise<PandasOnSparkDataFrame> {
    const result = await this.invoke('read_csv', path, options);
    const proxyValue = result as { __handle: JvmHandle };
    return new PandasOnSparkDataFrame(this.gateway, proxyValue.__handle);
  }

  async readParquet(path: string, options: Record<string, unknown> = {}): Promise<PandasOnSparkDataFrame> {
    const result = await this.invoke('read_parquet', path, options);
    const proxyValue = result as { __handle: JvmHandle };
    return new PandasOnSparkDataFrame(this.gateway, proxyValue.__handle);
  }

  async fromSpark(df: DataFrame): Promise<PandasOnSparkDataFrame> {
    const result = await this.invoke('DataFrame', df.toJvmHandle());
    const proxyValue = result as { __handle: JvmHandle };
    return new PandasOnSparkDataFrame(this.gateway, proxyValue.__handle);
  }

  sparkSession(): SparkSession {
    return this.spark;
  }

  async invoke(methodName: string, ...args: unknown[]): Promise<unknown> {
    const module = await this.gateway.getStatic('pyspark.pandas', '$MODULE$');
    return this.gateway.call(module, methodName, args);
  }
}
