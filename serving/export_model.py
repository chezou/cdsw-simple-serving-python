from data_eng.data_eng import read_data
from data_science.data_science import build_model

def export():
  training = read_data(generate=False)
  pipeline = build_model(training)
  
  target_path = "file:///tmp/spark-model"
  pipeline.write().overwrite().save(target_path)
  
if __name__ == "__main__":
  export()
