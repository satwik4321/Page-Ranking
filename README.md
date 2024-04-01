# Page-Ranking
This project aims at providing page ranks based on tf-idf of the source dataset and the query entered by the user. For this purpose pyspark was used which is an API for large-scale distributed systems framework known as Apache Spark.

CTF.py and CTF_query.py give you the page ranking results based on tf-idf whereas TF.py and TF_query.py give you the results based on logarithmic term frequencies. The documents have document ids which are in the form of html tags in the input files. The query is some text provided by the user

TF.py and CTF.py pre-process the data and create an intermediate form form for the frequency of the words for each document in the form: (word,[(docid1,freq1),(docid2,freq2),...])

TF_query.py and CTF_query.py give us the order of document IDs that are most relevant to the query provided.

Before executing the programs in this folder please upload your query file and input file to hdfs. The query is your own text file. The input file is provided

The command for this is: hdfs dfs -put "file_name"

The 4 files can be executed as follows and in the following order:

1. TF.py: spark-submit --master local --deploy-mode client TF.py
2. TF_query.py: spark-submit --master local --deploy-mode client TF_query.py /user/syamana/a1.txt (My input file containing documents was named as a1.txt. Also the path to the input file might change depending on where you save it)
3. CTF_query.py: spark-submit --master local --deploy-mode client CTF_query.py /user/syamana/a2.txt (This file contains the query. For the program to properly execute do not include any document IDs like the ones in the dataset. Please Chamge the name of the input file according to your convenience)
4. CTF.py: spark-submit --master local --deploy-mode client CTF.py

The final output is stored in qID on HDFS
