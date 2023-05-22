# mapreduce_wordfreq
Finding top 100 words using mapreduce

## How to run it
1. some simple command line for hdfs
```bash
hadoop fs -mkdir [-p] <hadoop_location>
hadoop fs -put <your_localfile_location> <hadoop_location>
hadoop fs -rm -r <hadoop_location>
```
2. first, run it locally to check
```bash
cat <your_datafile_path> | <mapper.py path> | sort -k1,1 | <reducer.py path>
```
3. run in hadoop
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.5.jar \
-file <mapper.py path> -mapper <mapper.py path> \
-file <reducer.py path> -reducer <reducer.py path> \
-file <combiner.py path> -combiner <combimer.py path>
-file <stopword.txt path> -input <hadoop_data_path> -output <hadoop_output>
```
4. check the result
```bash
 hadoop dfs -cat <hadoop_output>/part-00000   
```
