import { Node4jGateway, type Node4jClient } from './gateway';
import { createJvmProxy, type JvmHandle } from './proxy';
import { MLlib } from './mllib';
import { DataStreamReader, DataStreamWriter, StreamingQueryManager } from './streaming';
import { GraphFrames } from './graphframes';
import { PandasOnSpark } from './pandas_on_spark';

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

  get read(): DataFrameReader {
    const readerProxy = this.proxy.read() as { __handle: JvmHandle };
    return new DataFrameReader(this.gateway, readerProxy.__handle);
  }

  get catalog(): Catalog {
    const catalogProxy = this.proxy.catalog() as { __handle: JvmHandle };
    return new Catalog(this.gateway, catalogProxy.__handle);
  }

  async table(tableName: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'table', [tableName])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  get sparkContext(): unknown {
    return this.proxy.sparkContext();
  }

  get ml(): MLlib {
    return new MLlib(this.gateway);
  }

  get functions(): SqlFunctions {
    return new SqlFunctions(this.gateway);
  }

  get window(): Window {
    return new Window(this.gateway);
  }

  get readStream(): DataStreamReader {
    const readerProxy = this.proxy.readStream() as { __handle: JvmHandle };
    return new DataStreamReader(this.gateway, readerProxy.__handle);
  }

  get streams(): StreamingQueryManager {
    const managerProxy = this.proxy.streams() as { __handle: JvmHandle };
    return new StreamingQueryManager(this.gateway, managerProxy.__handle);
  }

  get graphframes(): GraphFrames {
    return new GraphFrames(this.gateway);
  }

  get pandasApi(): PandasOnSpark {
    return new PandasOnSpark(this.gateway, this);
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

  async join(other: DataFrame, on?: unknown, how?: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'join', [other.toJvmHandle(), on, how])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async withColumn(columnName: string, column: Column): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'withColumn', [columnName, column.toJvmHandle()])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  drop(...columns: unknown[]): unknown {
    return this.proxy.drop(...columns);
  }

  dropDuplicates(subset?: string[]): unknown {
    return this.proxy.dropDuplicates(subset);
  }

  orderBy(...columns: unknown[]): unknown {
    return this.proxy.orderBy(...columns);
  }

  sort(...columns: unknown[]): unknown {
    return this.proxy.sort(...columns);
  }

  limit(num: number): unknown {
    return this.proxy.limit(num);
  }

  distinct(): unknown {
    return this.proxy.distinct();
  }

  async union(other: DataFrame): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'union', [other.toJvmHandle()])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async unionByName(other: DataFrame, allowMissingColumns = false): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'unionByName', [other.toJvmHandle(), allowMissingColumns])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  repartition(...numPartitionsOrColumns: unknown[]): unknown {
    return this.proxy.repartition(...numPartitionsOrColumns);
  }

  coalesce(numPartitions: number): unknown {
    return this.proxy.coalesce(numPartitions);
  }

  cache(): unknown {
    return this.proxy.cache();
  }

  persist(storageLevel?: unknown): unknown {
    return this.proxy.persist(storageLevel);
  }

  unpersist(blocking = false): unknown {
    return this.proxy.unpersist(blocking);
  }

  count(): unknown {
    return this.proxy.count();
  }

  first(): unknown {
    return this.proxy.first();
  }

  head(n?: number): unknown {
    return this.proxy.head(n);
  }

  take(n: number): unknown {
    return this.proxy.take(n);
  }

  toJSON(): unknown {
    return this.proxy.toJSON();
  }

  toPandas(): unknown {
    return this.proxy.toPandas();
  }

  async createOrReplaceTempView(viewName: string): Promise<void> {
    await this.gateway.call(this.handle, 'createOrReplaceTempView', [viewName]);
  }

  async createGlobalTempView(viewName: string): Promise<void> {
    await this.gateway.call(this.handle, 'createGlobalTempView', [viewName]);
  }

  get write(): DataFrameWriter {
    const writerProxy = this.proxy.write() as { __handle: JvmHandle };
    return new DataFrameWriter(this.gateway, writerProxy.__handle);
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

  get writeStream(): DataStreamWriter {
    const writerProxy = this.proxy.writeStream() as { __handle: JvmHandle };
    return new DataStreamWriter(this.gateway, writerProxy.__handle);
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class DataFrameReader {
  private readonly gateway: Node4jGateway;
  private readonly handle: JvmHandle;
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  format(source: string): DataFrameReader {
    this.proxy.format(source);
    return this;
  }

  schema(schema: unknown): DataFrameReader {
    this.proxy.schema(schema);
    return this;
  }

  option(key: string, value: unknown): DataFrameReader {
    this.proxy.option(key, value);
    return this;
  }

  options(options: Record<string, unknown>): DataFrameReader {
    this.proxy.options(options);
    return this;
  }

  async load(path?: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'load', path ? [path] : [])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async json(path: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'json', [path])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async csv(path: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'csv', [path])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async parquet(path: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'parquet', [path])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async table(tableName: string): Promise<DataFrame> {
    const dataFrameHandle = (await this.gateway.call(this.handle, 'table', [tableName])) as JvmHandle;
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class DataFrameWriter {
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  format(source: string): DataFrameWriter {
    this.proxy.format(source);
    return this;
  }

  mode(saveMode: string): DataFrameWriter {
    this.proxy.mode(saveMode);
    return this;
  }

  option(key: string, value: unknown): DataFrameWriter {
    this.proxy.option(key, value);
    return this;
  }

  options(options: Record<string, unknown>): DataFrameWriter {
    this.proxy.options(options);
    return this;
  }

  partitionBy(...columns: string[]): DataFrameWriter {
    this.proxy.partitionBy(...columns);
    return this;
  }

  bucketBy(numBuckets: number, columnName: string, ...columnNames: string[]): DataFrameWriter {
    this.proxy.bucketBy(numBuckets, columnName, ...columnNames);
    return this;
  }

  sortBy(columnName: string, ...columnNames: string[]): DataFrameWriter {
    this.proxy.sortBy(columnName, ...columnNames);
    return this;
  }

  save(path?: string): unknown {
    return path ? this.proxy.save(path) : this.proxy.save();
  }

  saveAsTable(tableName: string): unknown {
    return this.proxy.saveAsTable(tableName);
  }

  json(path: string): unknown {
    return this.proxy.json(path);
  }

  csv(path: string): unknown {
    return this.proxy.csv(path);
  }

  parquet(path: string): unknown {
    return this.proxy.parquet(path);
  }

  insertInto(tableName: string): unknown {
    return this.proxy.insertInto(tableName);
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class Column {
  private readonly proxy;

  constructor(gateway: Node4jGateway, private readonly handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  alias(...aliases: string[]): unknown {
    return this.proxy.alias(...aliases);
  }

  asc(): unknown {
    return this.proxy.asc();
  }

  desc(): unknown {
    return this.proxy.desc();
  }

  cast(dataType: string): unknown {
    return this.proxy.cast(dataType);
  }

  otherwise(value: unknown): unknown {
    return this.proxy.otherwise(value);
  }

  over(windowSpec: WindowSpec): unknown {
    return this.proxy.over(windowSpec.toJvmHandle());
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class WindowSpec {
  private readonly proxy;

  constructor(gateway: Node4jGateway, private readonly handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  partitionBy(...columns: unknown[]): unknown {
    return this.proxy.partitionBy(...columns);
  }

  orderBy(...columns: unknown[]): unknown {
    return this.proxy.orderBy(...columns);
  }

  rowsBetween(start: number, end: number): unknown {
    return this.proxy.rowsBetween(start, end);
  }

  rangeBetween(start: number, end: number): unknown {
    return this.proxy.rangeBetween(start, end);
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }
}

export class Catalog {
  private readonly proxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.proxy = createJvmProxy(gateway, handle);
  }

  listDatabases(): unknown {
    return this.proxy.listDatabases();
  }

  listTables(dbName?: string): unknown {
    return dbName ? this.proxy.listTables(dbName) : this.proxy.listTables();
  }

  listColumns(tableName: string, dbName?: string): unknown {
    return dbName ? this.proxy.listColumns(tableName, dbName) : this.proxy.listColumns(tableName);
  }

  setCurrentDatabase(dbName: string): unknown {
    return this.proxy.setCurrentDatabase(dbName);
  }

  currentDatabase(): unknown {
    return this.proxy.currentDatabase();
  }

  cacheTable(tableName: string): unknown {
    return this.proxy.cacheTable(tableName);
  }

  uncacheTable(tableName: string): unknown {
    return this.proxy.uncacheTable(tableName);
  }

  clearCache(): unknown {
    return this.proxy.clearCache();
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class SqlFunctions {
  constructor(private readonly gateway: Node4jGateway) {}

  private async staticFunction(name: string, ...args: unknown[]): Promise<Column> {
    const functionHandle = await this.gateway.getStatic('org.apache.spark.sql.functions', name);
    const columnHandle = (await this.gateway.call(functionHandle, 'apply', args)) as JvmHandle;
    return new Column(this.gateway, columnHandle);
  }

  col(name: string): Promise<Column> {
    return this.staticFunction('col', name);
  }

  lit(value: unknown): Promise<Column> {
    return this.staticFunction('lit', value);
  }

  when(condition: Column, value: unknown): Promise<Column> {
    return this.staticFunction('when', condition.toJvmHandle(), value);
  }

  invoke(name: string, ...args: unknown[]): Promise<Column> {
    return this.staticFunction(name, ...args);
  }
}

export class Window {
  constructor(private readonly gateway: Node4jGateway) {}

  async partitionBy(...columns: unknown[]): Promise<WindowSpec> {
    const windowHandle = await this.gateway.getStatic('org.apache.spark.sql.expressions.Window', 'partitionBy');
    const specHandle = (await this.gateway.call(windowHandle, 'apply', columns)) as JvmHandle;
    return new WindowSpec(this.gateway, specHandle);
  }

  async orderBy(...columns: unknown[]): Promise<WindowSpec> {
    const windowHandle = await this.gateway.getStatic('org.apache.spark.sql.expressions.Window', 'orderBy');
    const specHandle = (await this.gateway.call(windowHandle, 'apply', columns)) as JvmHandle;
    return new WindowSpec(this.gateway, specHandle);
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
