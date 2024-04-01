#Import all the necessary packages
import math
import sys
import re
import nltk
from nltk.corpus import stopwords
from pyspark import SparkContext

sc=SparkContext("local","TF-IDF") #Initialize spark context

RDD=sc.textFile('/user/syamana/CTF_index/part-00000') #Load the word-frequencies stored in CTF_index

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

#Retrieve the term frequencies in the format specified and filter the ones needed for calculating dot product

def replace(line): #Replaces the strings with the original form
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

cnt=0
i=[]
for l in RDD8.collect():

   if cnt>=1:
      i.append(l[0])
   else:
      i=l
   cnt=cnt+1

def intext(x): #Filter the word frequencies required for the dot product
   if str(x[0]) in i:
      return 1
   else:
      return 0

RDD9=RDD.map(replace)

RDD10=RDD9.filter(intext)

def document_frequency(line): #Simply appends the document frequency
   cnt=0

   for i in line:
      cnt=cnt+1

   return (line,cnt)


def count_distinct(line): #This function counts the word frequency for each word in the query and divides it with the document frequency, i.e. the denominator for inverse document frequency
   words={} #The numerator of inverse document frequncy is multiplied during dot product when the query and total document frequncies are joined
   words1=[]
   for i in line:
      if i not in words:
         words[i]=1
      else:
         words[i]=words[i]+1
   for k in words.keys():
      words[k]=(1+math.log10(words[k])/words[k])
   for k,v in words.items():
      words1.append((k,v))
   return words1

RDDx3=RDD8.map(count_distinct)

RDDx=RDDx3.map(document_frequency)



def simplify(line): #Transforms the lines line into a desired form
   l=[]
   l1=[]

   for i in line[0]:
      l1.append(((i[0]),i[1]))
   l.append(l1)
   return l

RDDx1=RDDx.map(simplify)


def reduce1(line):
   l=[]
   l1=[]
   for i in line[1]:
      t=(line[0],i[1],len(line[1]))
      l.append((i[0],[t]))
      l1.append(l)
   return l

# Transformation of filtered term frequencies into desired form
RDDx4=RDD10.flatMap(reduce1)
RDDx5=RDDx4.reduceByKey(lambda x,y:x+y)
RDDx6=RDDx5.map(lambda x:[x[1]+[tuple(["",x[0]])]])
RDDx7=RDDx1.map(lambda x:[x])
q=RDDx1.first()
# Appending the query to the term frequency RDD
RDDx9=RDDx6.map(lambda x:q+x)
RDDx10=RDDx9.collect()
RDDx11=sc.parallelize(RDDx10)

def dot(x): #Dot product of each query and document is performed. Also the numerator value of the tf-idf of the query is multiplied here
   sum=0
   for i in x[0]:
      for j in x[1]:
         if i[0]==j[0]:
            sum=sum+(i[1]*j[2]*j[1])
   return (sum,x[1][-1][1])

RDDx12=RDDx11.map(dot)

RDDx13=RDDx12.sortByKey(lambda x:x) #Sort the documents by dot products
RDDxx=RDDx12.sortBy(lambda x:x,ascending=False) #Sort them in descending order

RDDx14=RDDxx.take(10) #Filter only the top 10 documents
RDDx15=sc.parallelize(RDDx14)
RDDx16=RDDx15.map(lambda x:(x[1]))
RDDx16.foreach(print)
RDDx16.saveAsTextFile('/user/syamana/qID')