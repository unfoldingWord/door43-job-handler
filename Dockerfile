FROM python:alpine

ADD . /code
WORKDIR /code

RUN pip install -r requirements.txt

EXPOSE 6379

CMD [ "rq", "worker", "-c", "rq_settings" ]
