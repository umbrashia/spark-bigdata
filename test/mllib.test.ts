import test from 'node:test';
import assert from 'node:assert/strict';

import { SparkSession } from '../src/pyspark';

function createNode4jRecorder() {
  const created: Array<{ className: string; args: unknown[] }> = [];
  const called: Array<{ method: string; args: unknown[] }> = [];
  const staticMembers: Array<{ className: string; memberName: string }> = [];

  return {
    created,
    called,
    staticMembers,
    client: {
      async newObject(className: string, ...args: unknown[]): Promise<unknown> {
        created.push({ className, args });
        return { __javaObjectId: className };
      },
      async getStaticMember(className: string, memberName: string): Promise<unknown> {
        staticMembers.push({ className, memberName });
        if (className === 'org.apache.spark.sql.SparkSession' && memberName === 'builder') {
          return { __javaObjectId: 'builder' };
        }

        return { __javaObjectId: `${className}.${memberName}` };
      },
      async call(_handle: unknown, methodName: string, ...args: unknown[]): Promise<unknown> {
        called.push({ method: methodName, args });

        if (methodName === 'getOrCreate') {
          return { __javaObjectId: 'spark' };
        }

        if (methodName === 'range') {
          return { __javaObjectId: 'df' };
        }

        if (methodName === 'fit') {
          return { __javaObjectId: 'pipeline-model' };
        }

        if (methodName === 'transform') {
          return { __javaObjectId: 'transformed-df' };
        }

        if (methodName === 'apply') {
          return { __javaObjectId: 'loaded-model' };
        }

        return { ok: true };
      },
    },
  };
}

test('SparkSession exposes expanded MLlib constructors for broad algorithm coverage', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  await spark.ml.logisticRegression();
  await spark.ml.randomForestClassifier();
  await spark.ml.gbtClassifier();
  await spark.ml.naiveBayes();
  await spark.ml.linearRegression();
  await spark.ml.decisionTreeRegressor();
  await spark.ml.als();
  await spark.ml.kMeans();
  await spark.ml.pca();

  const classNames = recorder.created.map((entry) => entry.className);
  assert.ok(classNames.includes('org.apache.spark.ml.classification.LogisticRegression'));
  assert.ok(classNames.includes('org.apache.spark.ml.classification.NaiveBayes'));
  assert.ok(classNames.includes('org.apache.spark.ml.regression.DecisionTreeRegressor'));
  assert.ok(classNames.includes('org.apache.spark.ml.recommendation.ALS'));
  assert.ok(classNames.includes('org.apache.spark.ml.feature.PCA'));
});

test('MLlib tuning wrappers support param grids and validator estimators', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();
  const inputDf = await spark.range(0, 10);

  const lr = await spark.ml.logisticRegression();
  const evaluator = await spark.ml.multiclassClassificationEvaluator();
  const paramGrid = await spark.ml.paramGridBuilder();
  paramGrid.addGrid('maxIter', [10, 20]).build();

  const cv = await spark.ml.crossValidator();
  cv.setEstimator(lr).setEvaluator(evaluator).setEstimatorParamMaps([{ maxIter: 10 }]);
  const cvModel = await cv.fit(inputDf);
  await cvModel.transform(inputDf);

  const tvs = await spark.ml.trainValidationSplit();
  tvs.setEstimator(lr).setEvaluator(evaluator).setEstimatorParamMaps([{ maxIter: 20 }]);
  const tvsModel = await tvs.fit(inputDf);
  await tvsModel.transform(inputDf);

  assert.ok(recorder.called.some((entry) => entry.method === 'addGrid'));
  assert.ok(recorder.called.some((entry) => entry.method === 'setEstimator'));
  assert.ok(recorder.called.some((entry) => entry.method === 'setEstimatorParamMaps'));
  assert.ok(recorder.called.some((entry) => entry.method === 'fit'));
  assert.ok(recorder.called.some((entry) => entry.method === 'transform'));
});

test('ML params ergonomics and model persistence wrappers delegate correctly', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const tokenizer = await spark.ml.tokenizer();
  tokenizer
    .set('inputCol', 'text')
    .setParams({ outputCol: 'words', toLowercase: true })
    .getParam('inputCol');
  tokenizer.getOrDefault('outputCol');
  tokenizer.explainParams();
  tokenizer.extractParamMap();
  tokenizer.uid();
  tokenizer.save('/tmp/tokenizer');

  const hashingTF = await spark.ml.hashingTF();
  hashingTF.write();

  await spark.ml.loadPipelineModel('/tmp/pipeline-model');
  await spark.ml.loadCrossValidatorModel('/tmp/cv-model');
  await spark.ml.loadTrainValidationSplitModel('/tmp/tvs-model');

  assert.ok(recorder.called.some((entry) => entry.method === 'save'));
  assert.ok(recorder.called.some((entry) => entry.method === 'explainParams'));
  assert.ok(recorder.called.some((entry) => entry.method === 'extractParamMap'));
  assert.ok(recorder.called.some((entry) => entry.method === 'uid'));
  assert.ok(recorder.called.some((entry) => entry.method === 'write'));
  assert.ok(recorder.staticMembers.some((entry) => entry.className === 'org.apache.spark.ml.PipelineModel' && entry.memberName === 'load'));
  assert.ok(recorder.staticMembers.some((entry) => entry.className === 'org.apache.spark.ml.tuning.CrossValidatorModel' && entry.memberName === 'load'));
});

test('Pipeline fit/transform flow delegates to JVM ML methods', async () => {
  const recorder = createNode4jRecorder();
  const spark = await SparkSession.builder(recorder.client).getOrCreate();

  const pipeline = await spark.ml.pipeline();
  const inputDf = await spark.range(0, 10);

  const model = await pipeline.fit(inputDf);
  const output = await model.transform(inputDf);

  assert.ok(output);
  assert.ok(recorder.called.some((entry) => entry.method === 'fit'));
  assert.ok(recorder.called.some((entry) => entry.method === 'transform'));
});
