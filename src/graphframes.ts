import { createJvmProxy, type JvmHandle } from './proxy';
import type { Node4jGateway } from './gateway';
import { DataFrame } from './pyspark';

export class GraphFrame {
  private readonly gateway: Node4jGateway;
  private readonly handle: JvmHandle;
  private readonly proxy;

  private async wrapDataFrameCall(methodName: string, args: unknown[] = []): Promise<DataFrame> {
    const result = await this.proxy[methodName](...args);
    const dfProxy = result as { __handle: JvmHandle };
    return new DataFrame(this.gateway, dfProxy.__handle);
  }

  private async wrapGraphCall(methodName: string, args: unknown[] = []): Promise<GraphFrame> {
    const result = await this.proxy[methodName](...args);
    const graphProxy = result as { __handle: JvmHandle };
    return new GraphFrame(this.gateway, graphProxy.__handle);
  }

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  get vertices(): DataFrame {
    const verticesProxy = this.proxy.vertices() as { __handle: JvmHandle };
    return new DataFrame(this.gateway, verticesProxy.__handle);
  }

  get edges(): DataFrame {
    const edgesProxy = this.proxy.edges() as { __handle: JvmHandle };
    return new DataFrame(this.gateway, edgesProxy.__handle);
  }

  async inDegrees(): Promise<DataFrame> {
    return this.wrapDataFrameCall('inDegrees');
  }

  async outDegrees(): Promise<DataFrame> {
    return this.wrapDataFrameCall('outDegrees');
  }

  async degrees(): Promise<DataFrame> {
    return this.wrapDataFrameCall('degrees');
  }

  async find(pattern: string): Promise<DataFrame> {
    return this.wrapDataFrameCall('find', [pattern]);
  }

  async motif(pattern: string): Promise<DataFrame> {
    return this.find(pattern);
  }

  async filterVertices(condition: string): Promise<GraphFrame> {
    return this.wrapGraphCall('filterVertices', [condition]);
  }

  async filterEdges(condition: string): Promise<GraphFrame> {
    return this.wrapGraphCall('filterEdges', [condition]);
  }

  async filterTriplets(condition: string): Promise<GraphFrame> {
    return this.wrapGraphCall('filterTriplets', [condition]);
  }

  async dropIsolatedVertices(): Promise<GraphFrame> {
    return this.wrapGraphCall('dropIsolatedVertices');
  }

  cache(): this {
    void this.proxy.cache();
    return this;
  }

  persist(storageLevel?: unknown): this {
    if (storageLevel === undefined) {
      void this.proxy.persist();
    } else {
      void this.proxy.persist(storageLevel);
    }

    return this;
  }

  unpersist(blocking = false): this {
    void this.proxy.unpersist(blocking);
    return this;
  }

  async bfs(fromExpr: string, toExpr: string, edgeFilter?: string, maxPathLength?: number): Promise<DataFrame> {
    const builder = this.proxy.bfs() as {
      fromExpr: (value: string) => unknown;
      toExpr: (value: string) => unknown;
      edgeFilter: (value: string) => unknown;
      maxPathLength: (value: number) => unknown;
      run: () => Promise<{ __handle: JvmHandle }>;
    };

    builder.fromExpr(fromExpr);
    builder.toExpr(toExpr);

    if (edgeFilter) {
      builder.edgeFilter(edgeFilter);
    }

    if (typeof maxPathLength === 'number') {
      builder.maxPathLength(maxPathLength);
    }

    const result = await builder.run();
    return new DataFrame(this.gateway, result.__handle);
  }

  async pageRank(config: { resetProbability?: number; tol?: number; maxIter?: number; sourceId?: string } = {}): Promise<GraphFrame> {
    const builder = this.proxy.pageRank() as {
      resetProbability: (value: number) => unknown;
      tol: (value: number) => unknown;
      maxIter: (value: number) => unknown;
      sourceId: (value: string) => unknown;
      run: () => Promise<{ __handle: JvmHandle }>;
    };

    if (typeof config.resetProbability === 'number') {
      builder.resetProbability(config.resetProbability);
    }

    if (typeof config.tol === 'number') {
      builder.tol(config.tol);
    }

    if (typeof config.maxIter === 'number') {
      builder.maxIter(config.maxIter);
    }

    if (typeof config.sourceId === 'string') {
      builder.sourceId(config.sourceId);
    }

    const result = await builder.run();
    return new GraphFrame(this.gateway, result.__handle);
  }

  async connectedComponents(config: { checkpointInterval?: number; broadcastThreshold?: number; algorithm?: string } = {}): Promise<DataFrame> {
    const builder = this.proxy.connectedComponents() as {
      checkpointInterval: (value: number) => unknown;
      broadcastThreshold: (value: number) => unknown;
      algorithm: (value: string) => unknown;
      run: () => Promise<{ __handle: JvmHandle }>;
    };

    if (typeof config.checkpointInterval === 'number') {
      builder.checkpointInterval(config.checkpointInterval);
    }

    if (typeof config.broadcastThreshold === 'number') {
      builder.broadcastThreshold(config.broadcastThreshold);
    }

    if (typeof config.algorithm === 'string') {
      builder.algorithm(config.algorithm);
    }

    const result = await builder.run();
    return new DataFrame(this.gateway, result.__handle);
  }

  async stronglyConnectedComponents(maxIter: number): Promise<DataFrame> {
    const builder = this.proxy.stronglyConnectedComponents() as {
      maxIter: (value: number) => unknown;
      run: () => Promise<{ __handle: JvmHandle }>;
    };

    builder.maxIter(maxIter);
    const result = await builder.run();
    return new DataFrame(this.gateway, result.__handle);
  }

  async labelPropagation(maxIter: number): Promise<DataFrame> {
    const builder = this.proxy.labelPropagation() as {
      maxIter: (value: number) => unknown;
      run: () => Promise<{ __handle: JvmHandle }>;
    };

    builder.maxIter(maxIter);
    const result = await builder.run();
    return new DataFrame(this.gateway, result.__handle);
  }

  async shortestPaths(landmarks: string[]): Promise<DataFrame> {
    const builder = this.proxy.shortestPaths() as {
      landmarks: (value: string[]) => unknown;
      run: () => Promise<{ __handle: JvmHandle }>;
    };

    builder.landmarks(landmarks);
    const result = await builder.run();
    return new DataFrame(this.gateway, result.__handle);
  }

  async triangleCount(): Promise<DataFrame> {
    return this.wrapDataFrameCall('triangleCount');
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }
}

export class GraphFrames {
  private readonly gateway: Node4jGateway;

  constructor(gateway: Node4jGateway) {
    this.gateway = gateway;
  }

  async create(vertices: DataFrame, edges: DataFrame): Promise<GraphFrame> {
    const handle = await this.gateway.create('org.graphframes.GraphFrame', [vertices.toJvmHandle(), edges.toJvmHandle()]);
    return new GraphFrame(this.gateway, handle);
  }

  async fromJvm(handle: JvmHandle): Promise<GraphFrame> {
    return new GraphFrame(this.gateway, handle);
  }

  async invokeStatic(methodName: string, ...args: unknown[]): Promise<unknown> {
    const module = await this.gateway.getStatic('org.graphframes.GraphFrame', '$MODULE$');
    return this.gateway.call(module, methodName, args);
  }
}
