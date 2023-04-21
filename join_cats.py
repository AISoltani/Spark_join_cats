from pyspark import SparkContext, SparkConf
from collections import namedtuple

conf = SparkConf().setMaster("local").setAppName("Amir-Soltani")
sc = SparkContext(conf=conf)
Record = namedtuple("Record",
                    ["videoID", "uploader", "age", "category", "length", "views", "rate", "ratings", "comments",
                     "relatedIDs"])

def parse_record(s):
    try:
        fields = s.split('\t')
        return Record(fields[0], fields[1], int(fields[2]), fields[3], int(fields[4]), int(fields[5]), float(fields[6]),
                      int(fields[7]), int(fields[8]), fields[9:])
    except:
        pass

parsed_data = sc.textFile("youtube.txt").map(parse_record).filter(lambda x: x != None).cache()
file = open("output.txt", 'a')
p2 = parsed_data.map(lambda x:[x.uploader, x.category]).distinct().groupByKey().filter(lambda x:len(x[1])>1).count() 
file.write(str(p2))
file.close()

