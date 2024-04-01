Before executing the programs in this folder please upload your query file and input file provided by you onto hdfs.
The command for this is: hdfs dfs -put "file_name"
file_name is the name of the input file

The 4 files can be executed as follows:

1. TF.py: spark-submit --master local --deploy-mode client TF.py
2. TF_query.py: spark-submit --master local --deploy-mode client TF_query.py /user/syamana/a1.txt (My input file containing documents was named as a1.txt. Also the path to the input file might change depending on where you save it)
3. CTF.py: spark-submit --master local --deploy-mode client CTF.py
4. CTF_query.py: spark-submit --master local --deploy-mode client CTF_query.py /user/syamana/a2.txt (This file contains the query. For the program to properly execute do not include any document IDs like the ones in the dataset. Please Chamge the name of the input file according to your convenience)

The outputs are stored in the folders with names specified int the assignment.