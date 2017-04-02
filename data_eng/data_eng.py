def read_data(generate=True):
  from pyspark.sql import SparkSession

  spark = SparkSession\
      .builder\
      .appName("Data engineering")\
      .getOrCreate()
              
  raw_input = "/tmp/datatraining.txt"
  csv_input = "/tmp/datatraining.csv"
  hdfs_csv_input = 'hdfs://{0}'.format(csv_input)
  
  from os import getenv
  from os import path
  from util.utility import HDFS
  
  hdfs_host = getenv('HDFS_HOST', 'localhost')
  hdfs_port = 14000
  hdfs = HDFS(hdfs_host, port=hdfs_port, secure=True)  
  
  if not generate and hdfs.exists(csv_input):
    return spark.read.csv(hdfs_csv_input,
                          inferSchema=True, header=True).drop("date")
  
  hdfs.client.delete(csv_input, recursive=True)

  spark.sparkContext.textFile('hdfs://{0}'.format(raw_input)) \
    .map(lambda l: l if l.startswith('"date"') else ','.join(l.split(',')[1:])) \
    .repartition(1) \
    .saveAsTextFile(hdfs_csv_input)

  return spark.read.csv(hdfs_csv_input,
                 inferSchema=True, header=True).drop("date")

if __name__ == "__main__":
  read_data()
