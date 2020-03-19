PROJECT=rele-saar
REGION=europe-west1
KEY=rele-saar-firebase-adminsdk


PUBSUB_TOPIC=projects/$(PROJECT)/topics/matan-test-pipe-topic

RUNNER=DirectRunner

export GOOGLE_APPLICATION_CREDENTIALS=/etc/relesaar/keys/$(KEY).json

run:
	python3 main.py	\
	--project ${PROJECT} \
	--region ${REGION} \
	--topic ${PUBSUB_TOPIC} \
	--streaming \