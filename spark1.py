from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("pyspark1")
sc = SparkContext(conf=conf)

sqlcontext = SQLContext(sc)
jdbcurl = "jdbc:mysql://quickstart.cloudera:3306/retail_db?user=retail_dba&password=cloudera"
df = sqlcontext.load(source="jdbc",url=jdbcurl,dbtable="departments")
for rec in df.collect():
	print(rec)
df.saveAsTextFile("/user/cloudera/departments")
