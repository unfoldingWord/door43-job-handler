FROM python:alpine

ADD . /code
WORKDIR /code

RUN pip install -r requirements.txt

EXPOSE 6379

# Define environment variables
# NOTE: The following environment variables are expected to be already set:
#	TX_DATABASE_PW
#	AWS_ACCESS_KEY_ID
#	AWS_SECRET_ACCESS_KEY
# NOTE: The following environment variables are optional:
#	REDIS_URL (can be omitted for testing to use a local instance)
#	DEBUG_MODE (can be set to any non-blank string to run in debug mode for testing)
#	GRAPHITE_URL (defaults to localhost if missing)
#	QUEUE_PREFIX (defaults to '', set to dev- for testing)
ENV QUEUE_PREFIX dev-
ENV DEBUG_MODE True
ENV GRAPHITE_URL dash.door43.org


CMD [ "rq", "worker", "-c", "rq_settings" ]

# NOTE: To build use: docker build -t d43jobhandler .
#       To test use: docker run --net="host" --rm d43jobhandler
