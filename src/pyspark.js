'use strict';

const { createJvmProxy } = require('./proxy');
const { Node4jGateway } = require('./gateway');

class SparkSessionBuilder {
  constructor(gateway) {
    this.gateway = gateway;
    this._options = new Map();
    this._enableHiveSupport = false;
  }

  appName(name) {
    this._options.set('spark.app.name', name);
    return this;
  }

  master(url) {
    this._options.set('spark.master', url);
    return this;
  }

  config(key, value) {
    this._options.set(key, value);
    return this;
  }

  enableHiveSupport() {
    this._enableHiveSupport = true;
    return this;
  }

  async getOrCreate() {
    const builderHandle = await this.gateway.getStatic('org.apache.spark.sql.SparkSession', 'builder');

    for (const [key, value] of this._options.entries()) {
      await this.gateway.call(builderHandle, 'config', [key, String(value)]);
    }

    if (this._enableHiveSupport) {
      await this.gateway.call(builderHandle, 'enableHiveSupport');
    }

    const sessionHandle = await this.gateway.call(builderHandle, 'getOrCreate');
    return new SparkSession(this.gateway, sessionHandle);
  }
}

class SparkSession {
  constructor(gateway, handle) {
    this.gateway = gateway;
    this.handle = handle;
    this._proxy = createJvmProxy(gateway, handle);
  }

  static builder(node4jClient) {
    return new SparkSessionBuilder(new Node4jGateway(node4jClient));
  }

  async sql(query) {
    const dataFrameHandle = await this.gateway.call(this.handle, 'sql', [query]);
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  async range(...args) {
    const dataFrameHandle = await this.gateway.call(this.handle, 'range', args);
    return new DataFrame(this.gateway, dataFrameHandle);
  }

  get sparkContext() {
    return this._proxy.sparkContext();
  }

  stop() {
    return this._proxy.stop();
  }

  /**
   * Full API fallback: any SparkSession method not implemented above is forwarded.
   */
  invoke(methodName, ...args) {
    return this._proxy[methodName](...args);
  }
}

class DataFrame {
  constructor(gateway, handle) {
    this.gateway = gateway;
    this.handle = handle;
    this._proxy = createJvmProxy(gateway, handle);
  }

  select(...columns) {
    return this._proxy.select(...columns);
  }

  filter(condition) {
    return this._proxy.filter(condition);
  }

  groupBy(...columns) {
    return new GroupedData(this.gateway, this._proxy.groupBy(...columns).__handle);
  }

  async collect() {
    return this._proxy.collect();
  }

  async show(numRows = 20, truncate = true, vertical = false) {
    return this._proxy.show(numRows, truncate, vertical);
  }

  /**
   * Full API fallback: call DataFrame/ Dataset methods not explicitly wrapped.
   */
  invoke(methodName, ...args) {
    return this._proxy[methodName](...args);
  }
}

class GroupedData {
  constructor(gateway, handle) {
    this.gateway = gateway;
    this.handle = handle;
    this._proxy = createJvmProxy(gateway, handle);
  }

  agg(...exprs) {
    return this._proxy.agg(...exprs);
  }

  count() {
    return this._proxy.count();
  }

  mean(...columns) {
    return this._proxy.mean(...columns);
  }

  sum(...columns) {
    return this._proxy.sum(...columns);
  }

  invoke(methodName, ...args) {
    return this._proxy[methodName](...args);
  }
}

class RDD {
  constructor(gateway, handle) {
    this.gateway = gateway;
    this.handle = handle;
    this._proxy = createJvmProxy(gateway, handle);
  }

  map(fn) {
    return this._proxy.map(fn);
  }

  filter(fn) {
    return this._proxy.filter(fn);
  }

  flatMap(fn) {
    return this._proxy.flatMap(fn);
  }

  reduce(fn) {
    return this._proxy.reduce(fn);
  }

  collect() {
    return this._proxy.collect();
  }

  invoke(methodName, ...args) {
    return this._proxy[methodName](...args);
  }
}

module.exports = {
  SparkSession,
  SparkSessionBuilder,
  DataFrame,
  GroupedData,
  RDD,
};
