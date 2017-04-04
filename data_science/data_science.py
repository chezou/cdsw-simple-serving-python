from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def build_model(training):
  #training = read_data()
  training.cache()
  
  columns = training.columns
  columns.remove("Occupancy")
  
  assembler = VectorAssembler(inputCols=columns, outputCol="featureVec")
  lr = LogisticRegression(featuresCol="featureVec", labelCol="Occupancy")
  
  pipeline = Pipeline(stages=[assembler, lr])
  
  param_grid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.0001, 0.001, 0.01, 0.1, 1.0]) \
    .build()
  
  evaluator = BinaryClassificationEvaluator(labelCol="Occupancy")
  
  validator = TrainValidationSplit(estimator=pipeline,
                             estimatorParamMaps=param_grid,
                             evaluator=evaluator,
                             trainRatio=0.9)
  
  validator_model = validator.fit(training)
  return validator_model.bestModel

if __name__ == "__main__":
  from data_eng.data_eng import read_data
  data = read_data(generate=False)
  build_model(data)
