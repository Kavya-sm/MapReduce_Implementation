from pyspark import SparkContext
from pyspark.sql import SparkSession

def mapper(record):
    try:
       
        mental_health_status = record['Mental_Health_Status']
        stress_level = record['Stress_Level']
        

        return [
            (mental_health_status, 1),
            (stress_level, 1)
        ]
    except Exception as e:
        print(f"Error processing record: {e}")
        return []

def reducer(a, b):
    return a + b

def main():
   
    spark = SparkSession.builder \
        .appName("MentalHealthMapReduce") \
        .getOrCreate()
    

    sc = spark.sparkContext
    
    df = spark.read.option("header", "true").csv("/Users/kavya/Downloads/data.csv")

    rdd = df.rdd
    

    mapped_rdd = rdd.flatMap(mapper)

  
    print("Sample mapped RDD elements:")
    for item in mapped_rdd.take(10):
        print(item)
    
  
    reduced_rdd = mapped_rdd.reduceByKey(reducer)
    

    results = reduced_rdd.collect()
    print("Results:")
    for result in results:
        print(result)
  
    spark.stop()

if __name__ == "__main__":
    main()
