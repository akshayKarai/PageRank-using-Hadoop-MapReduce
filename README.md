
## PageRank using Hadoop and MapReduce

[For the PageRank algorithm check here] (http://infolab.stanford.edu/~backrub/google.html)

Each page of Wikipedia is represented in XML as follows:
```
<title> Page_Name </title>
(other fields we do not care about)
<revision optionalAttr="val">
  <text optionalAttr2="val2"> 
    (Page body goes here)
  </text>
</revision>
```

#### Execution instructions - 

- Make directory in Cloudera named pagerank -
```
hadoop fs -mkdir /user/cloudera/pagerank /user/cloudera/pagerank/input 
```
- Put SimpleWiki in HDFS - 
```
hdfs fs -put /home/cloudera/Downloads/simplewiki.xml /simplewiki
```
- Compile the program of PageRank.java

Create jar file using 
```
jar -cvf pagerank.jar -C build/ . 
```
or
```
Create the jar file with name page rank.java using Eclipse

```
- Run the program - 
```
hadoop jar /home/cloudera/pagerank.jar PageRank /simplewiki /output
```
- Get the output file to local system - 
```
hadoop fs -cat /user/cloudera/pagerank/output/* >output.out 
```
- To get the top 100 entries use the below command -
```
head -100 output.out > output_100.out
```
- To delete any directory (output file in below ex.) -
```
hadoop fs -rm -r /user/cloudera/pagerank/output 
```

Cyber Duck can be used for transferring the files from local to cluster.
