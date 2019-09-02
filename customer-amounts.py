from pyspark import SparkContext, SparkConf
import collections

def parseLine(line):
    cells = line.split(',')
    return (int(cells[0]), float(cells[2]))

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

rdd = sc.textFile("customer-orders.csv")
custOrders = rdd.map(parseLine)
custAmounts = custOrders.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0]))
custAmountsSorted = custAmounts.sortByKey()
results = custAmountsSorted.collect()

for result in results:
    print(str(result[1]) + ": {:.2f}".format(result[0]))

'''
Output:

45: 3309.38
79: 3790.57
96: 3924.23
23: 4042.65
99: 4172.29
...
...
54: 6065.39
39: 6193.11
73: 6206.20
68: 6375.45
'''