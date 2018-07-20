XXXdoc: clean_doc
	echo 'building docs...'
	cd docs && sphinx-apidoc --force -M -P -e -o source/ ../enqueue
	cd docs && make html

XXXclean_doc:
	echo 'cleaning docs...'
	cd docs && rm -f source/enqueue
	cd docs && rm -f source/enqueue*.rst

dependencies:
	pip3 install --requirement requirements.txt

testDependencies:
	pip3 install --requirement test_requirements.txt
dependenciesTest:
	pip3 install --requirement test_requirements.txt

# NOTE: The following environment variables are expected to be set:
#	TX_DATABASE_PW
#	AWS_ACCESS_KEY_ID
#	AWS_SECRET_ACCESS_KEY

# NOTE: The following environment variables are optional:
#	REDIS_URL (can be omitted for testing to use a local instance)
#	DEBUG_MODE (can be set to any non-blank string to run in debug mode for testing)
#	GRAPHITE_URL (defaults to localhost if missing)
#	QUEUE_PREFIX (defaults to '', set to dev- for testing)

test:
	python3 -m unittest discover -s tests/

info:
	# Runs the rq info display with a one-second refresh
	rq info --interval 1

runDev:
	# This runs the rq job handler
	#   which removes and then processes jobs from the local redis dev- queue
	QUEUE_PREFIX="dev-" rq worker --config rq_settings

run:
	# This runs the rq job handler
	#   which removes and then processes jobs from the production redis queue
	# TODO: Can the AWS redis url go in here (i.e., is it public)?
	REDIS_URL="dadada" rq worker --config rq_settings

imageDev:
	docker build --tag unfoldingword/door43_job_handler:develop .

imageMaster:
	docker build --tag unfoldingword/door43_job_handler:master .

pushDevImage:
	# Expects to be already logged into Docker, e.g., docker login -u $(DOCKER_USERNAME)
	docker push unfoldingword/door43_job_handler:develop

pushMasterImage:
	# Expects to be already logged into Docker, e.g., docker login -u $(DOCKER_USERNAME)
	docker push unfoldingword/door43_job_handler:master
