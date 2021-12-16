FROM python

SHELL ["/bin/bash", "-c"]

COPY requirements.txt .
COPY serve.sh .
RUN chmod +x serve.sh

RUN pip3 install -r requirements.txt

COPY api.py .
RUN mkdir -p /expect-store/

COPY /expects/. /expect-store/

EXPOSE 5000

ENTRYPOINT ["./serve.sh"]