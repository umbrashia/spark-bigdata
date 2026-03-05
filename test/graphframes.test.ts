import test from 'node:test';
import assert from 'node:assert/strict';

import { SparkSession } from '../src/pyspark';

function createNode4jRecorder() {
  const created: Array<{ className: string; args: unknown[] }> = [];
  const calls: Array<{ method: string; args: unknown[] }> = [];

  return {
    created,
    calls,
    client: {
      async newObject(className: string, ...args: unknown[]): Promise<unknown> {
        created.push({ className, args });
        return { __javaObjectId: `${className}-instance` };
      },
      async getStaticMember(className: string, memberName: string): Promise<unknown> {
        if (className === 'org.apache.spark.sql.SparkSession' && memberName === 'builder') {
          return { __javaObjectId: 'builder' };
        }

        return { __javaObjectId: `${className}.${memberName}` };
      },
      async call(_handle: unknown, methodName: string, ...args: unknown[]): Promise<unknown> {
        calls.push({ method: methodName, args });

        if (methodName === 'getOrCreate') {
          return { __javaObjectId: 'spark' };
        }

        if (methodName === 'range') {
          return { __javaObjectId: 'df' };
        }

        if (['inDegrees', 'find', 'triangleCount', 'run'].includes(methodName)) {
          return { __javaObjectId: `df-${methodName}` };
        }

        if (['bfs', 'pageRank', 'connectedComponents', 'stronglyConnectedComponents', 'labelPropagation', 'shortestPaths'].includes(methodName)) {
          return { __javaObjectId: `${methodName}-builder` };
        }

        if (['filterVertices', 'filterEdges', 'filterTriplets', 'dropIsolatedVertices'].includes(methodName)) {
          return { __javaObjectId: 'graph-filtered' };
        }

        return { ok: true };
      },
    },
  };
}

test('GraphFrames create bridges JVM GraphFrame constructor', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();
  const vertices = await spark.range(0, 10);
  const edges = await spark.range(0, 20);

  const graph = await spark.graphframes.create(vertices, edges);
  const degrees = await graph.inDegrees();

  assert.ok(degrees);
  assert.ok(recorder.created.some((entry) => entry.className === 'org.graphframes.GraphFrame'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'inDegrees'));
});

test('GraphFrame advanced algorithms and motif helpers delegate to JVM builders', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();
  const vertices = await spark.range(0, 3);
  const edges = await spark.range(0, 3);
  const graph = await spark.graphframes.create(vertices, edges);

  const bfsResult = await graph.bfs('id = 1', 'id = 2', 'relationship = "friend"', 4);
  const prResult = await graph.pageRank({ resetProbability: 0.15, maxIter: 10 });
  const cc = await graph.connectedComponents({ checkpointInterval: 2, broadcastThreshold: 10, algorithm: 'graphframes' });
  const scc = await graph.stronglyConnectedComponents(5);
  const lp = await graph.labelPropagation(5);
  const sp = await graph.shortestPaths(['a', 'b']);
  const tc = await graph.triangleCount();
  const motif = await graph.motif('(a)-[e]->(b)');

  const filtered = await prResult.filterVertices('pagerank > 0.1');
  await filtered.filterEdges('src != dst');
  await filtered.filterTriplets('src.id != dst.id');
  await filtered.dropIsolatedVertices();
  filtered.cache().persist().unpersist();

  assert.ok(bfsResult);
  assert.ok(motif);
  assert.ok(cc);
  assert.ok(scc);
  assert.ok(lp);
  assert.ok(sp);
  assert.ok(tc);
  assert.ok(recorder.calls.some((entry) => entry.method === 'connectedComponents'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'stronglyConnectedComponents'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'labelPropagation'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'shortestPaths'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'triangleCount'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'landmarks'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'filterTriplets'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'cache'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'persist'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'unpersist'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'run'));
});
