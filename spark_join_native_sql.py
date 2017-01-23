from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

conf = SparkConf().setAppName("join_using_native_spark_sql")

sc= SparkContext(conf=conf)

sqlContext = SQLContext(sc)
sqlCOntext.sql("set spark.sql.shuffle.partitions =10")

ordersrdd = sc.textFile("/user/hive/warehouse/orders")
orderitemsrdd = sc.textFile("/user/hive/warehouse/order_items")

ordermap = ordersrdd.map(lambda o: o.split(','))
orderitemsmap = orderitemsrdd.map(lambda o:o.split(','))

orders = ordermap.map(lambda o: Row(order_id=int(o[0]), order_date=o[1], order_customer_id=int(o[2]), order_status=o[3]))
orderitems = orderitemsmap.map(lambda o: Row(order_item_id = int(o[0]), order_item_order_id=int(o[1]), order_item_product_id=int(o[2]), order_item_quantity=int(o[3]), order_item_subtotal=float(o[4]), order_item_product_price=float(o[5])))

orderSchema = sqlContext.inferSchema(orders)
orderitemsSchema = sqlContext.inferSchema(orderitems)

orderSchema.registerTempTable("orders")
orderitemsSchema.registerTempTable("order_items")

joinAggData = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal),count(distinct o.order_id) from orders o join order_items oi on o.order_id = oi.order_item_order_id group by o.order_date order by o.order_date")

for data in joinAggData.collect():
  print(data)
