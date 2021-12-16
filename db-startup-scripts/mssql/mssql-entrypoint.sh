#!/bin/bash
database=SAIL
wait_time=30s
password=$PASSWORD

# wait for SQL Server to come up
echo importing data will start in $wait_time...
echo $password
sleep $wait_time
echo importing data...

# run the init script to create the DB and the tables in /table
/opt/mssql-tools/bin/sqlcmd -S 0.0.0.0 -U sa -P $password -i ./mssql.sql

# import data to the tables
# TODO: make this a loop at some point
/opt/mssql-tools/bin/bcp SAIL.SAIL0000V.pedw_episode in "./data/pedw_episode.csv" -c -t',' -F 2 -S 0.0.0.0 -U sa -P $password

/opt/mssql-tools/bin/bcp SAIL.SAIL0000V.pedw_spell in "./data/pedw_spell.csv" -c -t',' -F 2 -S 0.0.0.0 -U sa -P $password

/opt/mssql-tools/bin/bcp SAIL.SAIL0000V.pedw_superspell in "./data/pedw_superspell.csv" -c -t',' -F 2 -S 0.0.0.0 -U sa -P $password

/opt/mssql-tools/bin/bcp SAIL.SAIL0000V.wdsd_main in "./data/wdsd_main.csv" -c -t',' -F 2 -S 0.0.0.0 -U sa -P $password

/opt/mssql-tools/bin/bcp SAIL.SAIL0000V.wlgp_event in "./data/wlgp_event.csv" -c -t',' -F 2 -S 0.0.0.0 -U sa -P $password

/opt/mssql-tools/bin/bcp SAIL.SAIL0000V.wlgp_reg in "./data/wlgp_reg.csv" -c -t',' -F 2 -S 0.0.0.0 -U sa -P $password
