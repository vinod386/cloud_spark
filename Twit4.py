# Spark example to print the average tweet length using Spark
# PGT April 2016  
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

from __future__ import print_function
import sys, json
from pyspark import SparkContext
#import plotly.plotly as py
#import plotly.graph_objs as go

# Given a full tweet object, return the text of the tweet
def getText(line):
  #try:
    js = json.loads(line)
    time = js['created_at'].split()
    #hsh_tag = js['entities']['hashtags']
    text = js['text'].encode('ascii', 'ignore')
    lengths=len(text)
    #lengths = text.map(lambda l: len(l))
    #hashh = []
    #for eachhash in js['entities']['hashtags']:
      #yield(eachhash['text'],1)
      #hashh.append(eachhash['text'].lower())
    #print (time[0])
    return (time[3].split(':')[0],lengths)
  #except Exception as a:
   # return

'''def gethour(line):
  #try:
    js = json.loads(line)
    time = js['created_at'].split()
    hsh_tag = js['entities']['hashtags']
    hashh = []
    for eachhash in js['entities']['hashtags']:
      #yield(eachhash['text'],1)
      hashh.append(eachhash['text'].lower())
    #print (time[0])
    return (time[3].split(':')[0],hashh)
'''
'''def hashcount(line):
    key= line[0]
    elem= line[1]
    elemdict=dict((i,elem.count(i)) for i in elem)
    #print (elemdict)
    return (key, max(elemdict,key=elemdict.get),max(elemdict.values()))
  #except Exception as a:
   # return
  '''
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
  sc = SparkContext(appName="heatmap")
  tweets = sc.textFile(sys.argv[1],)
  texts = tweets.map(getText).reduceByKey(lambda a,b: a+b)
  texts.coalesce(1).saveAsTextFile("lengths_twit4")
  #print (y.take(5))
  #print (x.take(5))
  #print (texts.take(10))
  
  #texts = tweets.map(getText)
  '''hour = tweets.map(gethour).reduceByKey(lambda a,b: a+b)
  #print(texts.take(2))
  texts_count= texts.map(hashcount)
  hour_count= hour.map(hashcount)
  output=texts_count.union(hour_count)
  output.coalesce(1).saveAsTextFile("lengths_twit6")
  #print(texts_count.take(2))
  #print(hour_count.take(2))
  #print (texts)
  #print (texts.take(5))
  #tweetcount =texts.map(lambda a : (a[0],[a[1][0],a[1][1]])).reduceByKey(lambda y, z: y[1]+z[1])'''
  '''tweetcount = texts.map(lambda a: (a[0],a[1][1])).reduceByKey(lambda a,b: (a+b))
  print (tweetcount.take(5))
  tweetlength = texts.map(lambda a: (a[0],a[1][0])).reduceByKey(lambda a,b: (a+b))
  print  ('others average\t',tweetlength.take(5)[1][1]/tweetcount.take(5)[1][1])
  print  ('PrezOno average\t',tweetlength.take(5)[0][1]/tweetcount.take(5)[0][1])
  sc.stop()'''
