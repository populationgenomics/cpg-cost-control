# pylint: disable=unused-import,missing-module-docstring
import os
import base64
import logging

import google.cloud.logging
from flask import Flask, request

from gcp import from_pubsub as gcp  # noqa: F401
from hail import from_pubsub as hail  # noqa: F401
from seqr import from_pubsub as seqr  # noqa: F401

app = Flask(__name__)

client = google.cloud.logging.Client()
client.setup_logging()


@app.route('/pubsub', methods=['POST'])
def pubsub():
    """
    pubsub endpoint test
    """
    logging.info('Testing a different endpoint')
    envelope = request.get_json()
    logging.info(f'Envelope: {envelope}')
    return ('Success', 201)


@app.route('/', methods=['POST'])
def index():
    """
    Entrypoint for the pubsub request for the Cloud Run job
    """

    logging.info('Processing pub/sub post request')

    try:
        envelope = request.get_json()
        if not envelope:
            msg = 'no Pub/Sub message received'
            logging.error(msg)
            return (f'Bad Request: {msg}', 400)

        if not isinstance(envelope, dict) or 'message' not in envelope:
            msg = 'invalid Pub/Sub message format'
            logging.error(msg)
            return (f'Bad Request: {msg}', 400)

        pubsub_message = envelope['message']
        logging.info(f'Message: {pubsub_message}')

        attributes = {}
        if isinstance(pubsub_message, dict) and 'attributes' in pubsub_message:
            attributes = (
                base64.b64decode(pubsub_message['attributes']).decode('utf-8').strip()
            )
        elif 'attributes' in envelope:
            attributes = envelope['attributes']

        func = attributes.get('function')
        logging.info(f'Function: {func}')
        logging.info(f'Attributes: {attributes}')

        if func == 'gcp':
            logging.info(f'Calling {func}')
            gcp(attributes, None)
        elif func == 'hail':
            logging.info(f'Calling {func}')
            hail(attributes, None)
        elif func == 'seqr':
            logging.info(f'Calling {func}')
            seqr(attributes, None)
        else:
            logging.error(f'Function {func} not found')
            return (f'Endpoint {func} not found', 404)

        logging.info('Success!')
        return ('Success!', 500)
    except Exception as exp:  # pylint: disable=broad-except
        logging.error(exp)
        return ('Server Error', 500)


if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host='0.0.0.0', port=PORT, debug=True)
