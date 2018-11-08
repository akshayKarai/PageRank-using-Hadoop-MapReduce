
***************************************************************************************************************
PageRank using Hadoop and MapReduce
***************************************************************************************************************

Execution instructions - 
----------------------------------------------------------------------------------------------------------
1. Make directory in dabs cluster named pagerank -
hadoop fs -mkdir /user/akarai/pagerank /user/akarai/pagerank/input 

2. Put SimpleWiki in HDFS - 
hdfs fs -put /home/akarai/Downloads/simplewiki.xml /simplewiki

3. Compile the program of PageRank.java
Create the jar file with name page rank.java using Eclipse

4. Run the program - 
hadoop jar /home/akarai/pagerank.jar PageRank /simplewiki /output

5. Get the output file to local system - 
hadoop fs -cat /user/akarai/pagerank/output/* >output.out 

6.To get the top 100 entries use the below command -
head -100 output.out > output_100.out

7.To delete any directory (output file in below ex.) -
hadoop fs -rm -r /user/akarai/pagerank/output 

----------------------------------------------------------------------------------------------------------
I have used Cyber Duck for transferring the files from local to dsba cluster.
