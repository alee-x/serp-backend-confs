#!/bin/bash
DATA_FILEPATH='data/*.csv'

echo "testing..."
for FILE in $DATA_FILEPATH; do
	fname=$(basename "$FILE");
	dir_give_name=$(echo $fname | cut -f 1 -d ".");
	echo $dir_give_name;
	echo "----";
done