# MapReduce_Implementation

# Introduction

This project leverages Apache Spark to demonstrate the MapReduce paradigm using a mental health dataset sourced from Kaggle. The dataset includes various attributes related to mental health, stress levels, and lifestyle factors. The primary goal of this analysis is to showcase how MapReduce can be applied to process and analyze large-scale data effectively. By utilizing PySpark’s distributed data processing capabilities, the project illustrates the practical implementation of MapReduce operations.

# MapReduce 
In this PySpark program, MapReduce is used to process and analyze a mental health dataset.

Map Phase: The dataset is grouped by attributes like Stress_Level, and counts are calculated for each group. This phase transforms the data into key-value pairs where the key is the attribute (e.g., Stress_Level) and the value is the count.

Reduce Phase: The intermediate results from the Map phase are aggregated to produce the final counts for each attribute. This phase combines the data to generate the output that shows the distribution of stress levels.


Dataset link:  https://www.kaggle.com/datasets/waqi786/mental-health-and-technology-usage-dataset
