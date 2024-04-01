# Page-Ranking
This project aims at providing page ranks based on tf-idf of the source dataset and the query entered by the user. For this purpose pyspark was used which is an API for large-scale distributed systems framework known as Apache Spark.

CTF.py and CTF_query.py give you the page ranking results based on tf-idf whereas TF.py and TF_query.py give you the results based on logarithmic term frequencies. The documents have document ids which are in the form of html tags in the input files. The query is some text provided by the user

TF.py and CTF.py pre-process the data and create an intermediate form form for the frequency of the words for each document in the form: (word,[(docid1,freq1),(docid2,freq2),...])

TF_query.py and CTF_query.py give us the order of document IDs that are most relevant to the query provided.
