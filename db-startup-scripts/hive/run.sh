#!/bin/bash
echo "waiting for hive-server to come up....";

HIVE_IP=`getent hosts hive-server | awk '{print $1}' | head -1`;
echo $HIVE_IP;

while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' ${HIVE_IP}:10002)" != "200" ]]; do 
	sleep 5; 
done

echo "adding data files"
beeline -n scott -p tiger -u "jdbc:hive2://${HIVE_IP}:10000/;transportMode=http;httpPath=cliservice" --incremental=true -f setup.hql