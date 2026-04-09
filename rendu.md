#2

On créé un fichier pour contenir nos données
```
hdfs dfs -mkdir -p /user/raj_ops/data

hdfs dfs -put tags.csv /user/raj_ops/data/
```

On créé le fichier python
```
nano traitement2.py
```
On exécute le python
```
python traitement2.py -r hadoop 
  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar 
  hdfs:///user/maria_dev/data/tags.csv 
  -o hdfs:///user/maria_dev/output_tags_count
```

On stock le résultat
```
hdfs dfs -cat /user/maria_dev/output_tags_count/part-00000
```
