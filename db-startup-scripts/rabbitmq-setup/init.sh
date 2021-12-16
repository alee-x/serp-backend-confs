#!/bin/bash

# Create Rabbitmq user
( rabbitmqctl wait --timeout 60 $RABBITMQ_PID_FILE ; 
rabbitmqctl add_user $RABBITMQ_DEFAULT_USER $RABBITMQ_DEFAULT_PASS 2>/dev/null ; 

rabbitmqctl add_vhost $RABBITMQ_DEFAULT_VHOST ;
rabbitmqctl add_vhost postgres_rmq ;
rabbitmqctl add_vhost db2_rmq ;
rabbitmqctl add_vhost mssql_rmq ;
rabbitmqctl add_vhost s3_rmq ;
rabbitmqctl add_vhost thost ;
rabbitmqctl add_vhost spark_rmq ;

rabbitmqctl set_user_tags $RABBITMQ_DEFAULT_USER administrator ; 
rabbitmqctl set_permissions -p $RABBITMQ_DEFAULT_VHOST $RABBITMQ_DEFAULT_USER  ".*" ".*" ".*" ; 

rabbitmqctl set_permissions -p postgres_rmq $RABBITMQ_DEFAULT_USER ".*" ".*" ".*" ; 
rabbitmqctl set_permissions -p db2_rmq $RABBITMQ_DEFAULT_USER ".*" ".*" ".*" ; 
rabbitmqctl set_permissions -p mssql_rmq $RABBITMQ_DEFAULT_USER ".*" ".*" ".*" ; 
rabbitmqctl set_permissions -p s3_rmq $RABBITMQ_DEFAULT_USER ".*" ".*" ".*" ; 
rabbitmqctl set_permissions -p thost $RABBITMQ_DEFAULT_USER ".*" ".*" ".*" ; 
rabbitmqctl set_permissions -p spark_rmq $RABBITMQ_DEFAULT_USER ".*" ".*" ".*" ; 

echo "*** User '$RABBITMQ_DEFAULT_USER' with password '$RABBITMQ_DEFAULT_PASS' completed. ***" ; 
echo "*** Log in the WebUI at port 15672 (example: http:/localhost:15672) ***") & 

rabbitmq-server $@