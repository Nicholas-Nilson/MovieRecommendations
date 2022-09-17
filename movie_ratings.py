from pyspark import SparkConf, SparkContext
import collections


# spark = SparkSession.builder().master("local[2]")
#           .appName("Movie Review Exploration")
#           .getOrCreate()
# df = spark.read.csv("/ml-latest-100k/reviews.csv")

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


# u.data columns
# userid movieID rating timestamp

lines = sc.textFile("ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f"{key}: {value}")