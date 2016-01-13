from pyspark import SparkContext
from pyspark.sql import SQLContext

sc= SparkContext(appName="TestApp")
sqlCtx = SQLContext(sc)

text_RDD = sc.textFile("testfile1")
text_RDD.collect()
def split_words(line): return line.split()
def create_pair(word): return (word,1)
pairs_RDD=text_RDD.flatMap(split_words).map(create_pair)

students = sc.parallelize([[100, "Alice", 8.5, "Computer Science"],
                          [101, "Bob", 7.1, "Engineering"],
                          [102, "Carl", 6.2, "Engineering"]
                          ])
def extract_grade(row): return row[2]
students.map(extract_grade).mean()
def extract_degree_grade(row): return (row[3], row[2])
degree_grade_RDD = students.map(extract_degree_grade)
degree_grade_RDD.collect()
degree_grade_RDD.reduceByKey(max).collect()
#"phoneNumbers": [{"type": "home","number": "212 555-1234"},{"type": "office","number": "646 555-4567"}],"children": [],"spouse": null}
students_df = sqlCtx.createDataFrame(students, ["id", "name", "grade", "degree"])
students_df.printSchema()
students_df.agg({"grade":"mean"}).collect()
students_df.groupBy("degree").max("grade").collect()
students_df.groupBy("degree").max("grade").show()

from pyspark.sql.types import *

schema = StructType([
StructField("id", LongType(), True),
StructField("name", StringType(), True),
StructField("grade", DoubleType(), True),
StructField("degree", StringType(), True) ])
students_df = sqlCtx.createDataFrame(students, schema)
students_json = [ '{"id":100, "name":"Alice", "grade":8.5, "degree":"Computer Science"}', '{"id":101, "name":"Bob", "grade":7.1, "degree":"Engineering"}']
with open("students.json", "w") as f:
 f.write("\n".join(students_json))
sqlCtx.jsonFile("students.json").show()
yelp_df = sqlCtx.load(source="com.databricks.spark.csv",header = 'true',
inferSchema = 'true',path = '/usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv')
yelp_df.printSchema()
yelp_df.filter(yelp_df.useful >= 1).count()
yelp_df.filter(yelp_df["useful"] >= 1).count()
yelp_df.filter("useful >= 1").count()
yelp_df.select("useful")
yelp_df.select("useful").agg({"useful":"max"}).collect()
yelp_df.select("id", "useful").take(5)
yelp_df.select("id", yelp_df.useful/28*100).show(5)
yelp_df.select("id", (yelp_df.useful/28*100).cast("int")).show(5)
useful_perc_data = yelp_df.select("id", (yelp_df.useful/28*100).cast("int"))
useful_perc_data.columns
useful_perc_data = yelp_df.select(yelp_df["id"].alias("uid"),(yelp_df.useful/28*100).cast("int").alias("useful_perc"))

from pyspark.sql.functions import asc, desc

useful_perc_data = yelp_df.select(yelp_df["id"].alias("uid"), (yelp_df.useful/28*100).cast("int").alias("useful_perc")).orderBy(desc("useful_perc"))
useful_perc_data.show()
useful_perc_data.join(yelp_df,yelp_df.id == useful_perc_data.uid,"inner").cache().select(useful_perc_data.uid, "useful_perc", "review_count").show()

################################

yelp_df.agg({"cool":"mean"}).collect()
trunc_df = yelp_df.filter("review_count>10 and open = 'True'")
(trunc_df.filter("stars = 5")).agg({"cool":"mean"}).collect()


trunc_df = yelp_df.filter("review_count>=10 and open = 'True'").groupBy("state").count()

trunc_df.orderBy(desc("count")).collect()

###################

/usr/lib/hue/apps/search/examples/collections/solr_configs_log_analytics_demo/index_data.csv
logs_df = sqlCtx.load(source="com.databricks.spark.csv",header = 'true',inferSchema = 'true',path ='index_data_http.csv')
sc._jsc.hadoopConfiguration().set('textinputformat.record.delimiter','\r\n')
sc._jsc.hadoopConfiguration().set('textinputformat.record.delimiter','\r\n')
from pyspark.sql.functions import asc, desc
logs_df.groupBy("code").count().orderBy(desc("count")).show()
logs_df.groupBy("code").avg("bytes").show()
import pyspark.sql.functions as F
logs_df.groupBy("code").agg(logs_df.code,F.avg(logs_df.bytes),F.min(logs_df.bytes),F.max(logs_df.bytes)).show()

###########################################
yelp_df = sqlCtx.load(source='com.databricks.spark.csv',header = 'true',inferSchema = 'true',path ='index_data.csv')
yelp_df.registerTempTable("yelp")
filtered_yelp = sqlCtx.sql("SELECT * FROM yelp WHERE useful >= 1")
filtered_yelp.count()

sqlCtx.sql("SELECT MAX(useful) AS max_useful FROM yelp").collect()
useful_perc_data.join(yelp_df,yelp_df.id == useful_perc_data.uid,"inner").select(useful_perc_data.uid, "useful_perc", "review_count")
useful_perc_data.registerTempTable("useful_perc_data")

sqlCtx.sql(
"""SELECT useful_perc_data.uid, useful_perc,
review_count
FROM useful_perc_data
INNER JOIN yelp
ON useful_perc_data.uid=yelp.id"""
)
customer_df = sqlCtx.sql("SELECT * FROM customers")
customer_df.show()
customer_df.printSchema()
sqlCtx.sql("""select c.category_name,
count(order_item_quantity) as count from order_items oi
inner join products p on oi.order_item_product_id =
p.product_id inner join categories c on c.category_id =
p.product_category_id group by c.category_name
order by count desc
limit 10"""
).show()
yelp_df.saveAsTable("yelp_reviews")

##################
orders_df = sqlCtx.sql("SELECT * FROM orders")
orders_df.filter("order_status = 'SUSPECTED_FRAUD'").count()

order_items_df = sqlCtx.sql("SELECT * from order_items")
rder_items = sqlCtx.createDataFrame(order_items_df.rdd, order_items_df.schema)
order_items.groupBy('order_item_order_id').sum('order_item_subtotal').orderBy(desc("SUM(order_item_subtotal#238)")).show(3)
orders = sqlCtx.createDataFrame(orders_df.rdd, orders_df.schema)
order_items.groupBy("order_item_order_id").sum("order_item_subtotal").orderBy(desc("SUM(order_item_subtotal#1452)")).show(4)
order_items = sqlCtx.createDataFrame(order_items.rdd, order_items.schema)
orders = sqlCtx.createDataFrame(orders.rdd, orders.schema)

joined_df = orders.join(order_items, orders.order_id == order_items.order_item_order_id, "inner").cache()
joined_df.filter("order_status = 'COMPLETE'").select("order_item_product_price").agg({"order_item_product_price" : "mean"}).collect()

#joined_df = orders.join(order_items, orders.order_id == order_items.order_item_order_id, "inner").cache()
oined_df.filter("order_status = 'COMPLETE'").groupBy("order_customer_id").agg({"order_item_subtotal": "sum"}).orderBy(desc("SUM(order_item_subtotal#268)")).first()
joined_df.filter("order_status != 'COMPLETE'").groupBy("order_id").agg({"order_item_subtotal": "sum"}).orderBy(desc("SUM(order_item_subtotal#268)")).first()
