FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

MAINTAINER Alex <a.s.lee@swansea.ac.uk>

ENV DATA_FILEPATH="/data/*"

ADD run.sh /run.sh
RUN chmod a+x /run.sh

RUN mkdir -p data
COPY data/* data/

CMD ["/run.sh"]
