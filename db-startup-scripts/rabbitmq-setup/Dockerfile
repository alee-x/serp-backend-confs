FROM rabbitmq:3.8-management-alpine

ENV RABBITMQ_PID_FILE=/var/lib/rabbitmq/mnesia/rabbitmq
ENV RABBITMQ_DEFAULT_USER=airflow_mq
ENV RABBITMQ_DEFAULT_PASS=airflow_mq
ENV RABBITMQ_DEFAULT_VHOST=airflow_mq_host

COPY init.sh /init.sh
RUN chmod +x /init.sh

EXPOSE 15672

CMD ["/bin/bash", "/init.sh"]