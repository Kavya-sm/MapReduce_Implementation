from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def mapper_mental_health(record):
    try:
        mental_health_status = record.get('Mental_Health_Status', 'Unknown')
        return [(mental_health_status, 1)]
    except Exception as e:
        print(f"Error processing record: {e}")
        return []

def mapper_stress_level(record):
    try:
        stress_level = record.get('Stress_Level', 'Unknown')
        return [(stress_level, 1)]
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


    rdd = df.rdd.map(lambda row: row.asDict()).filter(lambda x: 'Mental_Health_Status' in x and 'Stress_Level' in x)

    # Mapper Phase
    print("First few records:")
    for record in rdd.take(5):
        print(record)

    # Mental Health Status
    print("\nProcessing Mental Health Status:")
    mapped_rdd_mental_health = rdd.flatMap(mapper_mental_health)
    reduced_rdd_mental_health = mapped_rdd_mental_health.reduceByKey(reducer)
    print("Mental Health Status Counts:")
    results_mental_health = reduced_rdd_mental_health.collect()
    for result in results_mental_health:
        print(result)

    # Stress Level
    print("\nProcessing Stress Level:")
    mapped_rdd_stress_level = rdd.flatMap(mapper_stress_level)
    reduced_rdd_stress_level = mapped_rdd_stress_level.reduceByKey(reducer)
    print("Stress Level Counts:")
    results_stress_level = reduced_rdd_stress_level.collect()
    for result in results_stress_level:
        print(result)


    spark.stop()

if __name__ == "__main__":
    main()
