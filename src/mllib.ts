import { createJvmProxy, type JvmHandle } from './proxy';
import type { Node4jGateway } from './gateway';
import { DataFrame } from './pyspark';

const ML_CLASS = {
  Pipeline: 'org.apache.spark.ml.Pipeline',
  LogisticRegression: 'org.apache.spark.ml.classification.LogisticRegression',
  RandomForestClassifier: 'org.apache.spark.ml.classification.RandomForestClassifier',
  GBTClassifier: 'org.apache.spark.ml.classification.GBTClassifier',
  NaiveBayes: 'org.apache.spark.ml.classification.NaiveBayes',
  LinearRegression: 'org.apache.spark.ml.regression.LinearRegression',
  DecisionTreeRegressor: 'org.apache.spark.ml.regression.DecisionTreeRegressor',
  ALS: 'org.apache.spark.ml.recommendation.ALS',
  KMeans: 'org.apache.spark.ml.clustering.KMeans',
  PCA: 'org.apache.spark.ml.feature.PCA',
  Tokenizer: 'org.apache.spark.ml.feature.Tokenizer',
  HashingTF: 'org.apache.spark.ml.feature.HashingTF',
  IDF: 'org.apache.spark.ml.feature.IDF',
  VectorIndexer: 'org.apache.spark.ml.feature.VectorIndexer',
  Imputer: 'org.apache.spark.ml.feature.Imputer',
  VectorAssembler: 'org.apache.spark.ml.feature.VectorAssembler',
  StringIndexer: 'org.apache.spark.ml.feature.StringIndexer',
  OneHotEncoder: 'org.apache.spark.ml.feature.OneHotEncoder',
  StandardScaler: 'org.apache.spark.ml.feature.StandardScaler',
  CrossValidator: 'org.apache.spark.ml.tuning.CrossValidator',
  TrainValidationSplit: 'org.apache.spark.ml.tuning.TrainValidationSplit',
  ParamGridBuilder: 'org.apache.spark.ml.tuning.ParamGridBuilder',
  MulticlassClassificationEvaluator: 'org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator',
  RegressionEvaluator: 'org.apache.spark.ml.evaluation.RegressionEvaluator',
  ClusteringEvaluator: 'org.apache.spark.ml.evaluation.ClusteringEvaluator',
} as const;

type MlProxy = ReturnType<typeof createJvmProxy>;

export class MLBase {
  protected readonly gateway: Node4jGateway;
  protected readonly handle: JvmHandle;
  protected readonly proxy: MlProxy;

  constructor(gateway: Node4jGateway, handle: JvmHandle) {
    this.gateway = gateway;
    this.handle = handle;
    this.proxy = createJvmProxy(gateway, handle);
  }

  set(param: string, value: unknown): this {
    void this.proxy.set(param, value);
    return this;
  }

  setParams(params: Record<string, unknown>): this {
    for (const [key, value] of Object.entries(params)) {
      void this.proxy.set(key, value);
    }
    return this;
  }

  getParam(paramName: string): unknown {
    return this.proxy.getParam(paramName);
  }

  getOrDefault(paramName: string): unknown {
    return this.proxy.getOrDefault(paramName);
  }

  explainParams(): unknown {
    return this.proxy.explainParams();
  }

  extractParamMap(): unknown {
    return this.proxy.extractParamMap();
  }

  copy(extra?: unknown): this {
    return (extra === undefined ? this.proxy.copy() : this.proxy.copy(extra)) as this;
  }

  save(path: string): unknown {
    return this.proxy.save(path);
  }

  write(): unknown {
    return this.proxy.write();
  }

  uid(): unknown {
    return this.proxy.uid();
  }

  toJvmHandle(): JvmHandle {
    return this.handle;
  }

  invoke(methodName: string, ...args: unknown[]): unknown {
    return this.proxy[methodName](...args);
  }

  protected wrapDataFrameLike(value: unknown): unknown {
    if (value && typeof value === 'object' && '__isJvmProxy' in (value as Record<string, unknown>)) {
      const proxyValue = value as { __handle: JvmHandle };
      return new DataFrame(this.gateway, proxyValue.__handle);
    }

    return value;
  }
}

export class Transformer extends MLBase {
  async transform(df: DataFrame): Promise<DataFrame> {
    const result = await this.proxy.transform(df.toJvmHandle());
    return this.wrapDataFrameLike(result) as DataFrame;
  }
}

export class Estimator extends MLBase {
  async fit(df: DataFrame): Promise<Model> {
    const result = await this.proxy.fit(df.toJvmHandle());
    const modelProxy = result as { __handle: JvmHandle };
    return new Model(this.gateway, modelProxy.__handle);
  }
}

export class Model extends Transformer {}

export class PipelineModel extends Model {
  async transform(df: DataFrame): Promise<DataFrame> {
    return super.transform(df);
  }
}

export class Pipeline extends Estimator {
  setStages(stages: Array<Transformer | Estimator | Model>): this {
    void this.proxy.setStages(stages.map((stage) => stage.toJvmHandle()));
    return this;
  }

  async fit(df: DataFrame): Promise<PipelineModel> {
    const result = await this.proxy.fit(df.toJvmHandle());
    const modelProxy = result as { __handle: JvmHandle };
    return new PipelineModel(this.gateway, modelProxy.__handle);
  }
}

export class LogisticRegression extends Estimator {}
export class RandomForestClassifier extends Estimator {}
export class GBTClassifier extends Estimator {}
export class NaiveBayes extends Estimator {}
export class LinearRegression extends Estimator {}
export class DecisionTreeRegressor extends Estimator {}
export class ALS extends Estimator {}
export class KMeans extends Estimator {}

export class PCA extends Estimator {}
export class Tokenizer extends Transformer {}
export class HashingTF extends Transformer {}
export class IDF extends Estimator {}
export class VectorIndexer extends Estimator {}
export class Imputer extends Estimator {}
export class VectorAssembler extends Transformer {}
export class StringIndexer extends Estimator {}
export class OneHotEncoder extends Estimator {}
export class StandardScaler extends Estimator {}

export class Evaluator extends MLBase {
  evaluate(df: DataFrame): unknown {
    return this.proxy.evaluate(df.toJvmHandle());
  }
}

export class MulticlassClassificationEvaluator extends Evaluator {}
export class RegressionEvaluator extends Evaluator {}
export class ClusteringEvaluator extends Evaluator {}

export class ParamGridBuilder extends MLBase {
  addGrid(param: unknown, values: unknown[]): this {
    void this.proxy.addGrid(param, values);
    return this;
  }

  build(): unknown {
    return this.proxy.build();
  }
}

export class CrossValidatorModel extends Model {}
export class TrainValidationSplitModel extends Model {}

export class CrossValidator extends Estimator {
  setEstimator(estimator: Estimator): this {
    void this.proxy.setEstimator(estimator.toJvmHandle());
    return this;
  }

  setEvaluator(evaluator: Evaluator): this {
    void this.proxy.setEvaluator(evaluator.toJvmHandle());
    return this;
  }

  setEstimatorParamMaps(paramMaps: unknown): this {
    void this.proxy.setEstimatorParamMaps(paramMaps);
    return this;
  }

  async fit(df: DataFrame): Promise<CrossValidatorModel> {
    const result = await this.proxy.fit(df.toJvmHandle());
    const modelProxy = result as { __handle: JvmHandle };
    return new CrossValidatorModel(this.gateway, modelProxy.__handle);
  }
}

export class TrainValidationSplit extends Estimator {
  setEstimator(estimator: Estimator): this {
    void this.proxy.setEstimator(estimator.toJvmHandle());
    return this;
  }

  setEvaluator(evaluator: Evaluator): this {
    void this.proxy.setEvaluator(evaluator.toJvmHandle());
    return this;
  }

  setEstimatorParamMaps(paramMaps: unknown): this {
    void this.proxy.setEstimatorParamMaps(paramMaps);
    return this;
  }

  async fit(df: DataFrame): Promise<TrainValidationSplitModel> {
    const result = await this.proxy.fit(df.toJvmHandle());
    const modelProxy = result as { __handle: JvmHandle };
    return new TrainValidationSplitModel(this.gateway, modelProxy.__handle);
  }
}

export class MLlib {
  private readonly gateway: Node4jGateway;

  constructor(gateway: Node4jGateway) {
    this.gateway = gateway;
  }

  async create(className: string, ...args: unknown[]): Promise<MLBase> {
    const handle = await this.gateway.create(className, args);
    return new MLBase(this.gateway, handle);
  }

  private async createEstimator<T extends Estimator>(className: string, ctor: new (g: Node4jGateway, h: JvmHandle) => T): Promise<T> {
    return new ctor(this.gateway, await this.gateway.create(className));
  }

  private async createTransformer<T extends Transformer>(className: string, ctor: new (g: Node4jGateway, h: JvmHandle) => T): Promise<T> {
    return new ctor(this.gateway, await this.gateway.create(className));
  }

  async load<T extends MLBase>(className: string, path: string, ctor: new (g: Node4jGateway, h: JvmHandle) => T): Promise<T> {
    const classHandle = await this.gateway.getStatic(className, 'load');
    const modelHandle = (await this.gateway.call(classHandle, 'apply', [path])) as JvmHandle;
    return new ctor(this.gateway, modelHandle);
  }

  async pipeline(): Promise<Pipeline> { return this.createEstimator(ML_CLASS.Pipeline, Pipeline); }
  async logisticRegression(): Promise<LogisticRegression> { return this.createEstimator(ML_CLASS.LogisticRegression, LogisticRegression); }
  async randomForestClassifier(): Promise<RandomForestClassifier> { return this.createEstimator(ML_CLASS.RandomForestClassifier, RandomForestClassifier); }
  async gbtClassifier(): Promise<GBTClassifier> { return this.createEstimator(ML_CLASS.GBTClassifier, GBTClassifier); }
  async naiveBayes(): Promise<NaiveBayes> { return this.createEstimator(ML_CLASS.NaiveBayes, NaiveBayes); }
  async linearRegression(): Promise<LinearRegression> { return this.createEstimator(ML_CLASS.LinearRegression, LinearRegression); }
  async decisionTreeRegressor(): Promise<DecisionTreeRegressor> { return this.createEstimator(ML_CLASS.DecisionTreeRegressor, DecisionTreeRegressor); }
  async als(): Promise<ALS> { return this.createEstimator(ML_CLASS.ALS, ALS); }
  async kMeans(): Promise<KMeans> { return this.createEstimator(ML_CLASS.KMeans, KMeans); }
  async pca(): Promise<PCA> { return this.createEstimator(ML_CLASS.PCA, PCA); }
  async tokenizer(): Promise<Tokenizer> { return this.createTransformer(ML_CLASS.Tokenizer, Tokenizer); }
  async hashingTF(): Promise<HashingTF> { return this.createTransformer(ML_CLASS.HashingTF, HashingTF); }
  async idf(): Promise<IDF> { return this.createEstimator(ML_CLASS.IDF, IDF); }
  async vectorIndexer(): Promise<VectorIndexer> { return this.createEstimator(ML_CLASS.VectorIndexer, VectorIndexer); }
  async imputer(): Promise<Imputer> { return this.createEstimator(ML_CLASS.Imputer, Imputer); }
  async vectorAssembler(): Promise<VectorAssembler> { return this.createTransformer(ML_CLASS.VectorAssembler, VectorAssembler); }
  async stringIndexer(): Promise<StringIndexer> { return this.createEstimator(ML_CLASS.StringIndexer, StringIndexer); }
  async oneHotEncoder(): Promise<OneHotEncoder> { return this.createEstimator(ML_CLASS.OneHotEncoder, OneHotEncoder); }
  async standardScaler(): Promise<StandardScaler> { return this.createEstimator(ML_CLASS.StandardScaler, StandardScaler); }
  async crossValidator(): Promise<CrossValidator> { return this.createEstimator(ML_CLASS.CrossValidator, CrossValidator); }
  async trainValidationSplit(): Promise<TrainValidationSplit> { return this.createEstimator(ML_CLASS.TrainValidationSplit, TrainValidationSplit); }
  async paramGridBuilder(): Promise<ParamGridBuilder> { return new ParamGridBuilder(this.gateway, await this.gateway.create(ML_CLASS.ParamGridBuilder)); }

  async multiclassClassificationEvaluator(): Promise<MulticlassClassificationEvaluator> {
    const handle = await this.gateway.create(ML_CLASS.MulticlassClassificationEvaluator);
    return new MulticlassClassificationEvaluator(this.gateway, handle);
  }

  async regressionEvaluator(): Promise<RegressionEvaluator> {
    const handle = await this.gateway.create(ML_CLASS.RegressionEvaluator);
    return new RegressionEvaluator(this.gateway, handle);
  }

  async clusteringEvaluator(): Promise<ClusteringEvaluator> {
    const handle = await this.gateway.create(ML_CLASS.ClusteringEvaluator);
    return new ClusteringEvaluator(this.gateway, handle);
  }

  async loadPipelineModel(path: string): Promise<PipelineModel> {
    return this.load('org.apache.spark.ml.PipelineModel', path, PipelineModel);
  }

  async loadCrossValidatorModel(path: string): Promise<CrossValidatorModel> {
    return this.load('org.apache.spark.ml.tuning.CrossValidatorModel', path, CrossValidatorModel);
  }

  async loadTrainValidationSplitModel(path: string): Promise<TrainValidationSplitModel> {
    return this.load('org.apache.spark.ml.tuning.TrainValidationSplitModel', path, TrainValidationSplitModel);
  }
}
