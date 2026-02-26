import { createJvmProxy, type JvmHandle } from './proxy';
import type { Node4jGateway } from './gateway';
import { DataFrame } from './pyspark';

const ML_CLASS = {
  Pipeline: 'org.apache.spark.ml.Pipeline',
  LogisticRegression: 'org.apache.spark.ml.classification.LogisticRegression',
  RandomForestClassifier: 'org.apache.spark.ml.classification.RandomForestClassifier',
  GBTClassifier: 'org.apache.spark.ml.classification.GBTClassifier',
  LinearRegression: 'org.apache.spark.ml.regression.LinearRegression',
  KMeans: 'org.apache.spark.ml.clustering.KMeans',
  VectorAssembler: 'org.apache.spark.ml.feature.VectorAssembler',
  StringIndexer: 'org.apache.spark.ml.feature.StringIndexer',
  OneHotEncoder: 'org.apache.spark.ml.feature.OneHotEncoder',
  StandardScaler: 'org.apache.spark.ml.feature.StandardScaler',
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
export class LinearRegression extends Estimator {}
export class KMeans extends Estimator {}

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

export class MLlib {
  private readonly gateway: Node4jGateway;

  constructor(gateway: Node4jGateway) {
    this.gateway = gateway;
  }

  async create(className: string, ...args: unknown[]): Promise<MLBase> {
    const handle = await this.gateway.create(className, args);
    return new MLBase(this.gateway, handle);
  }

  async pipeline(): Promise<Pipeline> {
    const handle = await this.gateway.create(ML_CLASS.Pipeline);
    return new Pipeline(this.gateway, handle);
  }

  async logisticRegression(): Promise<LogisticRegression> {
    const handle = await this.gateway.create(ML_CLASS.LogisticRegression);
    return new LogisticRegression(this.gateway, handle);
  }

  async randomForestClassifier(): Promise<RandomForestClassifier> {
    const handle = await this.gateway.create(ML_CLASS.RandomForestClassifier);
    return new RandomForestClassifier(this.gateway, handle);
  }

  async gbtClassifier(): Promise<GBTClassifier> {
    const handle = await this.gateway.create(ML_CLASS.GBTClassifier);
    return new GBTClassifier(this.gateway, handle);
  }

  async linearRegression(): Promise<LinearRegression> {
    const handle = await this.gateway.create(ML_CLASS.LinearRegression);
    return new LinearRegression(this.gateway, handle);
  }

  async kMeans(): Promise<KMeans> {
    const handle = await this.gateway.create(ML_CLASS.KMeans);
    return new KMeans(this.gateway, handle);
  }

  async vectorAssembler(): Promise<VectorAssembler> {
    const handle = await this.gateway.create(ML_CLASS.VectorAssembler);
    return new VectorAssembler(this.gateway, handle);
  }

  async stringIndexer(): Promise<StringIndexer> {
    const handle = await this.gateway.create(ML_CLASS.StringIndexer);
    return new StringIndexer(this.gateway, handle);
  }

  async oneHotEncoder(): Promise<OneHotEncoder> {
    const handle = await this.gateway.create(ML_CLASS.OneHotEncoder);
    return new OneHotEncoder(this.gateway, handle);
  }

  async standardScaler(): Promise<StandardScaler> {
    const handle = await this.gateway.create(ML_CLASS.StandardScaler);
    return new StandardScaler(this.gateway, handle);
  }

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
}
