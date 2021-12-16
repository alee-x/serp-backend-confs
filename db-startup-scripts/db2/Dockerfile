FROM ibmcom/db2

USER root

ENV LICENSE=accept
ENV DB2INSTANCE=db2inst1
ENV DB2INST1_PASSWORD=MyP4ssW0rd
ENV DBNAME=SAIL
ENV BLU=false
ENV ENABLE_ORACLE_COMPATIBILITY=false
ENV UPDATEAVAIL=NO
ENV SAMPLEDB=false
ENV REPODB=false
ENV IS_OSXFS=true
ENV PERSISTENT_HOME=true
ENV HADR_ENABLED=false
ENV ETCD_ENDPOINT=
ENV ETCD_USERNAME=
ENV TCD_PASSWORD=

RUN mkdir /var/custom && \
	mkdir /var/custom/sql && \
	mkdir /var/custom/data 

COPY ./*.sql /var/custom/sql/
COPY ./data/*.csv /var/custom/data/
COPY db2-entrypoint.sh /var/custom/

RUN chmod 444 /var/custom/sql/*.sql  && \
	chmod 444 /var/custom/data/*.csv && \
	chmod a+x /var/custom/db2-entrypoint.sh