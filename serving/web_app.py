from flask import Flask, jsonify, render_template, request
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel


import json

MASTER = 'local'
APPNAME = 'simple-ml-serving'
MODEL_PATH = 'file:///home/cdsw/cdsw-simple-serving-python/model/spark-model'
#MODEL_PATH = 'file:///Users/ariga/work/cdsw-simple-serve-python/model/spark-model'

spark = SparkSession.builder.master(MASTER).appName(APPNAME).getOrCreate()
model = PipelineModel.load(MODEL_PATH)


def classify(input):
  #target_columns = input.columns + ["prediction"]
  target_columns = ["prediction"]
  return model.transform(input).select(target_columns).collect()

# webapp
app = Flask(__name__)


@app.route('/api/predict', methods=['POST'])
def predict():
  input_df = spark.sparkContext.parallelize([request.json]).toDF()
  output = classify(input_df)
  return jsonify(input=request.json, prediction=output)

@app.route('/')
def main():
    return render_template('index.html')


if __name__ == '__main__':
    app.run()
