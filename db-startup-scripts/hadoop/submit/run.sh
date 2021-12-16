#!/bin/bash
echo "adding data files"
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /project/SAIL0000V
$HADOOP_HOME/bin/hdfs dfs -put $DATA_FILEPATH /project/SAIL0000V/
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /shared/spark-logs

echo "adding for hive"
for FILE in $DATA_FILEPATH; do
	fname=$(basename "$FILE");
	dir_give_name=$(echo $fname | cut -f 1 -d ".");
	ftype=$(echo $fname | cut -f 2 -d ".");
	if [[ $ftype == 'csv' ]]; then
		$HADOOP_HOME/bin/hdfs dfs -mkdir -p /project/SAIL0000V/$dir_give_name
		$HADOOP_HOME/bin/hdfs dfs -put $FILE /project/SAIL0000V/$dir_give_name
	fi
done