from collections import Counter
import math
import sys
import re
import nltk
from nltk.corpus import stopwords
from pyspark import SparkContext

sc=SparkContext("local","TF-IDF")

RDD=sc.textFile('/user/syamana/TF_index/part-00000')
inputRDD=sc.textFile(sys.argv[2]) #Read the query

tags=re.compile("<.*?>")

doc_id=re.compile(r"<docid>")

doc_id1=re.compile(r"</docid>")

punctuations=re.compile("[^\w\s ]")

spaces=re.compile(' ')

#Perform processing on the text

inputRDD1=inputRDD.map(lambda line: re.sub(tags,' ',line))

inputRDD2=inputRDD1.map(lambda line: re.sub(punctuations,' ',line))

inputRDD3=inputRDD2.map(lambda line: re.sub(spaces,' ',line))

inputRDD4=inputRDD3.map(lambda line: line.lower())

RDD4=inputRDD4.map(lambda line: re.sub(doc_id,' ',line))

RDD12=RDD4.map(lambda line: re.sub(doc_id1,' ',line))

RDD5=RDD12.collect()

RDD6=sc.parallelize(RDD5)

RDD7=RDD6.map(lambda line: line.split())

stop_words=stopwords.words('english')

#Remove the stopwords for the query
def rem_stop(word):
   stop_words=stopwords.words('english')
   w=[]
   for words in word:
      if words not in stop_words:
         w.append(words)
   return w

RDD8=RDD7.map(rem_stop)

#RDD.foreach(print)

cnt=0

RDD8.foreach(print)

print("-------\n\n\n\n\n")

def count_distinct(line):
   b=[]
   for word in line:
      b.append(word)
   return b

RDDx2=RDD8.map(count_distinct)
#RDDx2.foreach(print)
print("\n\n\n")

for l in RDDx2.collect():
   #print(l)
   if cnt>=1:
      i.append(l[0])
   else:
      i=l
   cnt=cnt+1
#RDDx3=RDD8.collect()
#RDDx3.foreach(print)
def replace(line):
   word=""
   number=""
   a=()
   c=()
   d=()
   b=[]
   start=0
   start1=0
   for char in line:
      if start==0:
         if char!='@':
            if char.isalpha() or char.isdigit():
               word=word+char
         else:
            c=((word))
            start=1
      else:
         if char=='#':
            if '.' in number:
               n=float(number)
            else:
               n=int(number)
            if len(a)==0:
               a=d+(n,)
               d=()
            else:
               d=a+(n,)
               a=()
            number=""
         elif char=='+':
            if '.' in number:
               n=float(number)
            else:
               n=int(number)
            n=float(number)
            if len(a)==0:
               a=d+(n,)
               d=()
               b.append(a)
            else:
               d=a+(n,)
               a=()
               b.append(d)
            d=()
            a=()
            number=""
         if char.isalpha() or char.isdigit() or char=='.':
            number=number+char

   if '.' in number:
        n=float(number)
   else:
      n=int(number)

   if len(a)==0:
      a=d+(n,)
      d=()
      b.append(a)
   else:
      d=a+(n,)
      a=()
      b.append(d)
   f=[b]
   e=(word,)+tuple(f,)
   return e


RDD9=RDD.map(replace)

def intext(x):
   if str(x[0]) in i:
      return 1
   else:
      return 0

RDD10=RDD9.filter(intext)

'''
RDD10=RDD9.map(lambda x:re.sub('\#',',',x))
RDD11=RDD10.map(lambda x:re.sub('\@',',[(',x))
RDD12=RDD11.map(lambda x:re.sub('\)\)',')])',x))
#RDD12.foreach(print)
RDD13=RDD9.reduceByKey(lambda x:x)
'''

#RDD10.foreach(print)
def reduce(line):
   summ=0
   a=[]
   for i in line[1]:
      a.append((i[0],i[1]))
   return a

RDD11=RDD10.flatMap(reduce)
RDD12=RDD11.reduceByKey(lambda x,y:x+y)
RDD13=RDD12.sortBy(lambda x:x[1])
RDD14=RDD13.take(10)
RDD15=sc.parallelize(RDD14)
#RDD15.foreach(print)
RDD15.saveAsTextFile('/user/syamana/Task1Output')