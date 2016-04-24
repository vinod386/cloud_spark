# Spark example to print the average tweet length using Spark
# PGT April 2016   
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

from __future__ import print_function
import sys, json
from pyspark import SparkContext

# Given a full tweet object, return the text of the tweet
def getText(line):
  try:
    js = json.loads(line)
    if js['user']['screen_name']=='PrezOno':
      text = js['text'].encode('ascii', 'ignore')
      return ['Prez_Ono',[len(text),1]]
    else:
      text = js['text'].encode('ascii', 'ignore')
      return ['other',[len(text),1]]
  except Exception as a:
    return []
  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
  sc = SparkContext(appName="PrezOnoavgtweetlength")
  tweets = sc.textFile(sys.argv[1],)
  texts = tweets.map(getText)
  #print (texts.take(5))
  #tweetcount =texts.map(lambda a : (a[0],[a[1][0],a[1][1]])).reduceByKey(lambda y, z: y[1]+z[1])
  tweetcount = texts.map(lambda a: (a[0],a[1][1])).reduceByKey(lambda a,b: (a+b))
  print (tweetcount.take(2))
  tweetlength = texts.map(lambda a: (a[0],a[1][0])).reduceByKey(lambda a,b: (a+b))
  print  ('PrezOno average\t',tweetlength.take(2)[1][1]/tweetcount.take(2)[1][1])
  print  ('others average\t',tweetlength.take(2)[0][1]/tweetcount.take(2)[0][1])
  
  #lenghths.saveAsTextFile("Qsn3_lengths_twit3")
  sc.stop()
  #count=0
  #count1=0
  #l_Ono=0
  #l_others=0
  #if texts[0]=='PrezOno':
'''  
    count+=1
    lengths = texts.map(lambda l: len(l))
    l_Ono=l_Ono+lengths
    print (l_Ono)
  else texts[0]=='other':
    count1+=1
    lengths = texts.map(lambda l: len(l))
    l_others=l_others+lengths
    print (l_others)
  #if texts['user']['screen_name']=='PrezOno':
   #   P=texts['text']
    #  lc_Ono=len(P)
     # wc_Ono=wc_Ono+lc_Ono
      #count_Ono=count_Ono+1 
      #print '%s' % (count_Ono)
  #else:
      #tw=texts['text']
      #lc_others=len(tw)
      #wc_other=wc_other+lc_others
      #count_other=count_other+1
     # continue 
  #lengths = texts.map(lambda l: len(l))
  
  # Just show 10 tweet lengths to validate this works
  #print(lengths.take(10))
  # Print out the stats
  #print(lengths.stats())
  
  # Save to your local HDFS folder
  lengths.saveAsTextFile("lengths")
  
  '''


