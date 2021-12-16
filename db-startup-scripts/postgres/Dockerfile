FROM postgres:latest

EXPOSE 5432
ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=MyP4ssW0rd
ENV POSTGRES_DB=SAIL

COPY postgres.sh /docker-entrypoint-initdb.d
COPY postgres.sql /docker-entrypoint-initdb.d
COPY /data/* /docker-entrypoint-initdb.d/data/

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["postgres"]