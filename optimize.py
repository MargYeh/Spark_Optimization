'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''

#%%
import findspark
import os
import time
findspark.init('C:/spark/spark-3.5.3-bin-hadoop3', edit_rc=True)
os.environ['SPARK_HOME'] = 'C:/spark/spark-3.5.3-bin-hadoop3'
os.environ['HADOOP_HOME'] = 'C:/hadoop'

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

#%%
spark = SparkSession.builder.appName('Optimize I').getOrCreate()

base_path = os.getcwd()

project_path = ('/').join(base_path.split('/')[0:-3]) 

answers_input_path = os.path.join(project_path, 'data/answers')

questions_input_path = os.path.join(project_path, 'data/questions')

answersDF = spark.read.option('path', answers_input_path).load()

questionsDF = spark.read.option('path', questions_input_path).load()

'''
Answers aggregation

Here we : get number of answers per question per month
'''
#%%
answers_month = answersDF.withColumn('month', month('creation_date')) \
    .groupBy('question_id', 'month') \
    .agg(count('*') \
    .alias('cnt'))

#%%
# resultDF = questionsDF.join(answers_month, 'question_id') \
#     .select('question_id', 'creation_date', 'title', 'month', 'cnt')

# resultDF.orderBy('question_id', 'month').show()

# '''
# Task:

# see the query plan of the previous result and rewrite the query to optimize it
# '''
# # %%
# #Physical plan of original
# resultDF.explain()

# # %%
# #Caching answers_month
# answers_month.cache()
# resultDF_cached = questionsDF.join(answers_month, 'question_id') \
#     .select('question_id', 'creation_date', 'title', 'month', 'cnt')

# resultDF_cached.orderBy('question_id', 'month').show()
# answers_month.unpersist()
# #%%
# resultDF_cached.explain()

#%%
#Repartitioning answers_month
r_answers_month = answers_month.repartition(10, 'question_id')

resultDF_r = questionsDF.join(r_answers_month, 'question_id') \
    .select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF_r.orderBy('question_id', 'month').show()

#%%
# resultDF_r.explain()

#%%
# #This is so the job shows up in Spark UI
time.sleep(1000000)

