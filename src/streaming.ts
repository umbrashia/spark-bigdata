import { createJvmProxy, type JvmHandle } from './proxy';
import type { Node4jGateway } from './gateway';
import { DataFrame } from './pyspark';

export class DataStreamReader {
  private readonly gateway: Node4jGateway;
  private readonly handle: JvmHandle;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  format(source: string): this {
    void this.proxy.format(source);
    return this;
  }

  schema(schemaDefinition: unknown): this {
    void this.proxy.schema(schemaDefinition);
    return this;
  }

  option(key: string, value: unknown): this {
    void this.proxy.option(key, String(value));
    return this;
  }

  options(values: Record<string, unknown>): this {
    for (const [key, value] of Object.entries(values)) {
      void this.proxy.option(key, String(value));
    }

    return this;
  }

  async load(path?: string): Promise<DataFrame> {
    const result = path ? await this.proxy.load(path) : await this.proxy.load();
    const dfProxy = result as { __handle: JvmHandle };
    return new DataFrame(this.gateway, dfProxy.__handle);
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }
}

export class DataStreamWriter {
  private readonly gateway: Node4jGateway;
  private readonly handle: JvmHandle;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  format(source: string): this {
    void this.proxy.format(source);
    return this;
  }

  outputMode(mode: string): this {
    void this.proxy.outputMode(mode);
    return this;
  }

  option(key: string, value: unknown): this {
    void this.proxy.option(key, String(value));
    return this;
  }

  options(values: Record<string, unknown>): this {
    for (const [key, value] of Object.entries(values)) {
      void this.proxy.option(key, String(value));
    }

    return this;
  }

  queryName(name: string): this {
    void this.proxy.queryName(name);
    return this;
  }

  trigger(triggerConfig: unknown): this {
    void this.proxy.trigger(triggerConfig);
    return this;
  }

  partitionBy(...columns: string[]): this {
    void this.proxy.partitionBy(...columns);
    return this;
  }

  async start(path?: string): Promise<StreamingQuery> {
    const result = path ? await this.proxy.start(path) : await this.proxy.start();
    const queryProxy = result as { __handle: JvmHandle };
    return new StreamingQuery(this.gateway, queryProxy.__handle);
  }

  async toTable(tableName: string): Promise<StreamingQuery> {
    const result = await this.proxy.toTable(tableName);
    const queryProxy = result as { __handle: JvmHandle };
    return new StreamingQuery(this.gateway, queryProxy.__handle);
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }
}

export class StreamingQuery {
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  awaitTermination(timeoutMs?: number): unknown {
    if (typeof timeoutMs === 'number') {
      return this.proxy.awaitTermination(timeoutMs);
    }

    return this.proxy.awaitTermination();
  }

  processAllAvailable(): unknown {
    return this.proxy.processAllAvailable();
  }

  stop(): unknown {
    return this.proxy.stop();
  }

  status(): unknown {
    return this.proxy.status();
  }

  recentProgress(): unknown {
    return this.proxy.recentProgress();
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class StreamingQueryManager {
  private readonly gateway: Node4jGateway;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.proxy = createJvmProxy(gateway, handle);
  }

  active(): unknown {
    return this.proxy.active();
  }

  async get(id: string): Promise<StreamingQuery | null> {
    const result = await this.proxy.get(id);

    if (!result) {
      return null;
    }

    const queryProxy = result as { __handle: JvmHandle };
    return new StreamingQuery(this.gateway, queryProxy.__handle);
  }

  awaitAnyTermination(timeoutMs?: number): unknown {
    if (typeof timeoutMs === 'number') {
      return this.proxy.awaitAnyTermination(timeoutMs);
    }

    return this.proxy.awaitAnyTermination();
  }

  resetTerminated(): unknown {
    return this.proxy.resetTerminated();
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}
