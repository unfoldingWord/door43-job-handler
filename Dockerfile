FROM python:alpine

ADD . /code
WORKDIR /code

RUN pip3 install --requirement requirements.txt

# EXPOSE 6379

# Define environment variables
# NOTE: The following environment variables are expected to be set:
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


CMD [ "rq", "worker", "--config", "rq_settings" ]

# NOTE: To build use: docker build --tag door43_job_handler .
#
#       To test use: docker run --env TX_DATABASE_PW --env AWS_ACCESS_KEY_ID --env AWS_SECRET_ACCESS_KEY --net="host" --name door43_job_handler --rm door43_job_handler
#           (The above assumes that the three confidential environment variables are already set in the current environment
