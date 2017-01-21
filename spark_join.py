from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("Spark_Join_App")
sc = SparkContext(conf=conf)

ordersrdd = sc.textFile("/user/hive/warehouse/orders")
orderitemsrdd = sc.textFile("/user/hive/warehouse/order_items")
ordersmap = ordersrdd.map(lambda x : (x.split(',')[0], x))
orderitemsmap = orderitemsrdd.map(lambda x : (x.split(',')[1], x))

for i in ordersmap.take(5):
	print(i)

for i in orderitemsmap.take(5):
	print(i)

orderjoin = orderitemsmap.join(ordersmap)

orderdtqt = orderjoin.map(lambda x:(x[1][1].split(',')[1],float(x[1][0].split(',')[4])))

revenueperday = orderdtqt.reduceByKey(lambda x,y:x+y)


ordersperday = orderjoin.map(lambda x : (str(x[0]) + "," + x[1][1].split(',')[1]))

distinctordersperday = ordersperday.distinct()

totalOrdersPerDay = distinctordersperday.map(lambda x: (x.split(',')[1],1)).reduceByKey(lambda x,y:x+y)

finalresult = revenueperday.join(totalOrdersPerDay)

finalresult.saveAsTextFile("finaljoin")




