FROM mcr.microsoft.com/mssql/server:2019-latest

EXPOSE 1433
ENV PASSWORD=MyP4ssW0rd
ENV SA_PASSWORD=MyP4ssW0rd
ENV ACCEPT_EULA=Y

USER root

COPY mssql-entrypoint.sh /usr/src/app/
COPY mssql.sql /usr/src/app/
COPY /data/* /usr/src/app/data/

WORKDIR /usr/src/app/

RUN chmod +x ./mssql-entrypoint.sh

ENTRYPOINT ["/bin/bash", "-c", "./mssql-entrypoint.sh & /opt/mssql/bin/sqlservr"]