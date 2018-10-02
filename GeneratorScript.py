import string
from pyspark import SparkConf, SparkContext
import pandas as pd
import os
import sys



conf = SparkConf().setMaster("local").setAppName("SongCount")
sc = SparkContext(conf=conf)

punctuation1= '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'

column_name = sys.argv[1]

print("Processing Column: "+column_name)
lyrics_df = pd.read_csv("lyrics.csv")

f =open('lyrics.txt', 'w+', encoding="utf-8")

for index,row in lyrics_df.iterrows():
    f.write(str(row[column_name]))


lines = sc.textFile ("lyrics.txt")
lines = lines.map(lambda line: line.lower())
map = lines.flatMap(lambda line : line.translate(line.maketrans('','',string.punctuation)).split(" ")).map(lambda word: (word,1))
count = map.reduceByKey(lambda a, b: a + b)
count = count.map(lambda x:(x[1],x[0])).sortByKey(False)
count = count.map(lambda x:(x[0],x[1]))


#count.leftOuterJoin(count_line)
count.repartition(1).saveAsTextFile("result")

thisFile = "./result/part-00000"
base = os.path.splitext(thisFile)[0]
os.rename(thisFile, base + ".txt")
print("Done. Check "+thisFile + ".txt")
