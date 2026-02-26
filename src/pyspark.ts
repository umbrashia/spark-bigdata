import { Node4jGateway, type Node4jClient } from './gateway';
import { createJvmProxy, type JvmHandle } from './proxy';
import { MLlib } from './mllib';

export class SparkSessionBuilder {
  private readonly gateway: Node4jGateway;
  private readonly options = new Map<string, unknown>();
  private hiveSupportEnabled = false;

  constructor(gateway: Node4jGateway) {
    this.gateway = gateway;
  }

  appName(name: string): SparkSessionBuilder {
    this.options.set('spark.app.name', name);
    return this;
  }

  master(url: string): SparkSessionBuilder {
    this.options.set('spark.master', url);
    return this;
  }

  config(key: string, value: unknown): SparkSessionBuilder {
    this.options.set(key, value);
    return this;
  }

  enableHiveSupport(): SparkSessionBuilder {
    this.hiveSupportEnabled = true;
    return this;
  }

  async getOrCreate(): Promise<SparkSession> {
    const builderHandle = await this.gateway.getStatic('org.apache.spark.sql.SparkSession', 'builder');

    for (const [key, value] of this.options.entries()) {
      await this.gateway.call(builderHandle, 'config', [key, String(value)]);
    }

    if (this.hiveSupportEnabled) {
      await this.gateway.call(builderHandle, 'enableHiveSupport');
    }

    const sessionHandle = (await this.gateway.call(builderHandle, 'getOrCreate')) as JvmHandle;
    return new SparkSession(this.gateway, sessionHandle);
  }
}

export class SparkSession {
  private readonly gateway: Node4jGateway;
  private readonly handle: JvmHandle;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  static builder(node4jClient: Node4jClient): SparkSessionBuilder {
    return new SparkSessionBuilder(new Node4jGateway(node4jClient));
  }

  async sql(query: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'sql', [query])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async range(...args: number[]): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'range', args)) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  get sparkContext(): unknown {
    return this.proxy.sparkContext();
  }

  get ml(): MLlib {
    return new MLlib(this.gateway);
  }

  stop(): unknown {
    return this.proxy.stop();
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class DataFrame {
  private readonly gateway: Node4jGateway;
  private readonly handle: JvmHandle;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  select(...columns: unknown[]): unknown {
    return this.proxy.select(...columns);
  }

  filter(condition: unknown): unknown {
    return this.proxy.filter(condition);
  }

  groupBy(...columns: unknown[]): GroupedData {
    const groupedProxy = this.proxy.groupBy(...columns) as { __handle: JvmHandle };
    return new GroupedData(this.gateway, groupedProxy.__handle);
  }

  collect(): Promise<unknown> {
    return this.proxy.collect() as Promise<unknown>;
  }

  show(numRows = 20, truncate: boolean | number = true, vertical = false): Promise<unknown> {
    return this.proxy.show(numRows, truncate, vertical) as Promise<unknown>;
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class GroupedData {
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  agg(...exprs: unknown[]): unknown {
    return this.proxy.agg(...exprs);
  }

  count(): unknown {
    return this.proxy.count();
  }

  mean(...columns: string[]): unknown {
    return this.proxy.mean(...columns);
  }

  sum(...columns: string[]): unknown {
    return this.proxy.sum(...columns);
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class RDD {
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  map(fn: unknown): unknown {
    return this.proxy.map(fn);
  }

  filter(fn: unknown): unknown {
    return this.proxy.filter(fn);
  }

  flatMap(fn: unknown): unknown {
    return this.proxy.flatMap(fn);
  }

  reduce(fn: unknown): unknown {
    return this.proxy.reduce(fn);
  }

  collect(): unknown {
    return this.proxy.collect();
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}
