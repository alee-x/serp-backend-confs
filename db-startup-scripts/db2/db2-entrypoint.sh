#!/bin/bash
database=SAIL
wait_time=15s
PASSWORD=$DB2INST1_PASSWORD

export PATH=/database/config/db2inst1/sqllib/bin/:$PATH

db2 connect to $DBNAME user $DB2INSTANCE using $PASSWORD

# create tables
db2 -tvf /var/custom/sql/db2.sql

db2 import from /var/custom/data/pedw_spell.csv of del insert into SAIL0000V.PEDW_SPELL
db2 import from /var/custom/data/pedw_episode.csv of del insert into SAIL0000V.PEDW_EPISODE
db2 import from /var/custom/data/pedw_superspell.csv of del insert into SAIL0000V.PEDW_SUPERSPELL
db2 import from /var/custom/data/wdsd_main.csv of del insert into SAIL0000V.WDSD_MAIN
db2 import from /var/custom/data/wlgp_event.csv of del insert into SAIL0000V.WLGP_EVENT
db2 import from /var/custom/data/wlgp_reg.csv of del insert into SAIL0000V.WLGP_REG

db2 terminate
