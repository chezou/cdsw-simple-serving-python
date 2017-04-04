from flask import Flask, jsonify, render_template, request
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel


import json

MASTER = 'local'
APPNAME = 'simple-ml-serving'
MODEL_PATH = 'file:///home/sense/model/spark-model2'

spark = SparkSession.builder.master(MASTER).appName(APPNAME).getOrCreate()
model = PipelineModel.load(MODEL_PATH)


def classify(input):
  #target_columns = input.columns + ["prediction"]
  target_columns = ["prediction"]
  return model.transform(input_df).select(target_columns).collect()

# webapp
app = Flask(__name__)


@app.route('/api/predict', methods=['POST'])
def predict():
  input_json = json.loads(request.json)
  input_RDD = spark.sparkContext.parallelize([input_json])
  input_df = spark.read.json(input_RDD)
  output = classify(input_df)
  return jsonify(retuls=output)

@app.route('/')
def main():
    return render_template('index.html')


if __name__ == '__main__':
    app.run()