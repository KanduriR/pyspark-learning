from pyspark import SparkContext, SparkConf
import csv

conf = SparkConf().setMaster('local').setAppName('customerExpenditure')
sc = SparkContext(conf=conf)


def getKeyValue(line):
    # each entry has comma seperated values of <custid, transaction id, amount>
    data = line.split(',')
    return (data[0], float(data[2]))


lines = sc.textFile('resources/customer-orders.csv')
data = lines.map(getKeyValue)  # returns a (id, amt) for each transaction
cust_sums = data.reduceByKey(lambda x, y: x + y).collect()  # sums each customers sales amount


with open('output/customer_expenditure.csv', 'w') as fd:
    writer = csv.writer(fd)

    writer.writerow(['ID', 'total_sale'])
    writer.writerows(sorted(cust_sums, key=lambda num: num[1], reverse=True))
