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

        if (methodName === 'inDegrees' || methodName === 'find' || methodName === 'run') {
          return { __javaObjectId: `df-${methodName}` };
        }

        if (methodName === 'bfs' || methodName === 'pageRank') {
          return { __javaObjectId: `${methodName}-builder` };
        }

        if (methodName === 'filterVertices') {
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

test('GraphFrame bfs and pageRank builders delegate to run()', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();
  const vertices = await spark.range(0, 3);
  const edges = await spark.range(0, 3);
  const graph = await spark.graphframes.create(vertices, edges);

  const bfsResult = await graph.bfs("id = 1", "id = 2", 'relationship = "friend"', 4);
  const prResult = await graph.pageRank({ resetProbability: 0.15, maxIter: 10 });
  const filtered = await prResult.filterVertices('pagerank > 0.1');
  const matches = await filtered.find('(a)-[e]->(b)');

  assert.ok(bfsResult);
  assert.ok(matches);
  assert.ok(recorder.calls.some((entry) => entry.method === 'bfs'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'pageRank'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'run'));
  assert.ok(recorder.calls.some((entry) => entry.method === 'find'));
});
