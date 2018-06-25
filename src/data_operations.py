from pyspark.sql import SparkSession, Row
from pyspark import SparkContext
from itertools import islice

# test with DataFrame
spark = SparkSession \
    .builder \
    .appName("Basic operation with structured data") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
'''
df = spark.read.csv("../data/iris.csv", header=True, inferSchema=True)
# Displays the content of the DataFrame to stdout

# two = df.filter(df["SepalLength"] == 6.0)
# NUM_ROWS = df.count()
# header = list(df.columns)
# f = df.select("SepalLength")
# print(type(f))
# print(header[1])
#
# # df.describe().show()
# # df.printSchema()
# print(df.count())

numerical_cols = df.drop("Name")
averages = numerical_cols.groupBy().avg()
df.describe().show()
averages.show()

averages.repartition(1).write.csv("../data/iris", mode="append", header=True)

'''


def sum_row(a, b):
    for i in xrange(len(a)):
        a[i] += b[i]
    return a


# Test with RDD
sc = spark.sparkContext
# sc1 = SparkContext()

lines = sc.textFile(name="../data/iris.csv")  # doc file text thanh 1 collection cac line
parts = lines.map(lambda l: l.split(","))  # moi dong tach thanh list phan cach boi dau ","
parts_drop_header = parts.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
iris_numeric = parts_drop_header.map(lambda p: p[0:-1]).map(lambda p: map(float, p))  # lay 4 phan tu moi dau moi dong de tinh toan

print(iris_numeric.collect())
NUM_ROWS = iris_numeric.count()
SUM = iris_numeric.reduce(sum_row)

averages = [SUM[i] * 1.0 / NUM_ROWS for i in xrange(len(SUM))]

print(averages)
