#Import the required files
from collections import Counter
import math
import sys
import re
import nltk
from nltk.corpus import stopwords
from pyspark import SparkContext

sc=SparkContext("local","TF-IDF")

inputRDD=sc.textFile("/user/syamana/a1.txt") #Load the file
#Perform text preprocessing
tags=re.compile("<.*?>")

doc_id=re.compile(r"<docid>")

doc_id1=re.compile(r"</docid>")

punctuations=re.compile("[^\w\s ]")

spaces=re.compile(' ')

inputRDD1=inputRDD.map(lambda line: re.sub(tags,' ',line))

inputRDD2=inputRDD1.map(lambda line: re.sub(punctuations,' ',line))

inputRDD3=inputRDD2.map(lambda line: re.sub(spaces,' ',line))

inputRDD4=inputRDD3.map(lambda line: line.lower())

RDD4=inputRDD4.map(lambda line: re.sub(doc_id,' ',line))

RDD12=RDD4.map(lambda line: re.sub(doc_id1,' ',line))

RDD5=RDD12.collect()

RDD6=sc.parallelize(RDD5)

RDD7=RDD6.map(lambda line: line.split())

RDD71=RDD7.collect()

stop_words=stopwords.words('english')
#Remove stop words
def rem_stop(word):
   stop_words=stopwords.words('english')
   w=[]
   for words in word:
      if words not in stop_words:
         w.append(words)
   return w

RDD8=RDD7.map(rem_stop)
#Calculate the normalizing constant value for each document

def count_distinct(line):
   dist=set(line)
   words=Counter(line[1:-1])
   a=[]
   b=[]
   c=[]
   id=line[0]
   sum=0
   sum1=0
   for i in words:
      sum=0
      for j in words:
         if i==j:
            sum=sum+1
      sum1=sum1+pow(sum,2)
   sum1=pow(sum1,0.5)
   for i in line[1:-1]:
      if i not in words:
         words[i]=1
      else:
         words[i]=words[i]+1
   for word in line[1:-1]:
         a=[int(id),1,word,sum1]
         b=[]
         b.append((int(id),word))
         b.append(a)
         c.append(b)
   return c
RDD9=RDD8.flatMap(count_distinct)
RDD9.foreach(print)
RDD10=RDD9.reduceByKey(lambda x,y: [x[0],x[1]+y[1],x[2],x[3] if (len(x)>1 and len(y)>1) else [x[0],x[1]]]) #Count the occurences of each word
RDD10.foreach(print)
RDDx1=RDD10.map(lambda x:[((x[1][2])),(x[1][0],x[1][1]/x[1][3])]) #Normalize using the constant
#RDDx1.foreach(print)
RDDx=RDDx1.map(lambda x:[x[0],(x[1][0],((1+math.log10(x[1][1]))))]) #Applying logarithm for the frequencies
#RDDx.foreach(print)
RDD11=RDDx.map(lambda x: (x[0],[tuple(x[1])])) #Simplification of the tuple

RDD12=RDD11.reduceByKey(lambda x,y:x+y) #Clubbing the word frequencies based on their words from different documents
#Converting to a string in the required form
RDD13=RDD12.map(lambda x:str(x))
RDD14=RDD13.map(lambda x:re.sub(r', \[\(','@',x))
RDD15=RDD14.map(lambda x:re.sub(r'\)\, \(','+',x))
RDD16=RDD15.map(lambda x:re.sub(r',','#',x))
RDD17=RDD16.map(lambda x:re.sub(r'\]','',x))
RDD18=RDD17.map(lambda x:re.sub(r' ','',x))
RDD18.foreach(print)
RDD18.saveAsTextFile('/user/syamana/CTF_index')