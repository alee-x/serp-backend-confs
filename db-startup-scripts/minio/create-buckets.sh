#!/bin/sh

echo "sleeping";
sleep 30;
echo "awake";
/usr/bin/mc config host add mymin http://$DEFAULT_MINIO_URL:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD;
/usr/bin/mc mb --ignore-existing mymin/frontdoor;
/usr/bin/mc policy set public mymin/frontdoor;
cd testdata;
/usr/bin/mc cp -r * mymin/frontdoor;
/usr/bin/mc mb --ignore-existing mymin/sail0000v;
/usr/bin/mc policy set public mymin/sail0000v;
/usr/bin/mc cp -r * mymin/sail0000v;
/usr/bin/mc mb --ignore-existing mymin/serp-sail/cvst/jobs/dataset1_workflow3_ledger98312/;
/usr/bin/mc policy set public mymin/serp-sail;
/usr/bin/mc cp data.csv mymin/serp-sail/cvst/jobs/dataset1_workflow3_ledger98312/;
/usr/bin/mc cp ledger.json mymin/serp-sail/cvst/jobs/dataset1_workflow3_ledger98312/;

/usr/bin/mc mb --ignore-existing mymin/serp-sail/cvst/spark-workflow/dataset1/;
cd ../partdata;
/usr/bin/mc cp spark-ledger.json mymin/serp-sail/cvst/spark-workflow/;
/usr/bin/mc cp -r *.csv mymin/serp-sail/cvst/spark-workflow/dataset1;

/usr/bin/mc mb --ignore-existing mymin/af-logs/temp;
/usr/bin/mc policy set public mymin/af-logs;

/usr/bin/mc mb --ignore-existing mymin/spark-hs/logs;
/usr/bin/mc policy set public mymin/spark-hs;

/usr/bin/mc mb --ignore-existing mymin/pseudo;
/usr/bin/mc policy set public mymin/pseudo;
cd ../statdata;
/usr/bin/mc cp -r *.dta mymin/pseudo/stata/;
/usr/bin/mc cp -r *.do mymin/pseudo/stata/;
/usr/bin/mc cp -r *.sav mymin/pseudo/spss/;
/usr/bin/mc cp -r *.csv mymin/pseudo/test/;