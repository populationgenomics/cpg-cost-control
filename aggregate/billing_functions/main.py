# pylint: disable=unused-import,missing-module-docstring
import os
import base64
from flask import Flask, request

from utils import logger
from gcp import from_pubsub as gcp  # noqa: F401
from hail import from_pubsub as hail  # noqa: F401
from seqr import from_pubsub as seqr  # noqa: F401

app = Flask(__name__)

logger = logger.getChild('pubsub-router')


@app.route('/', methods=['POST'])
def index():
    """
    Entrypoint for the pubsub request for the Cloud Run job
    """

    logger.info('Processing pub/sub post request')

    try:
        envelope = request.get_json()
        if not envelope:
            msg = 'no Pub/Sub message received'
            logger.error(msg)
            return f'Bad Request: {msg}', 400

        if not isinstance(envelope, dict) or 'message' not in envelope:
            msg = 'invalid Pub/Sub message format'
            logger.error(msg)
            return f'Bad Request: {msg}', 400

        pubsub_message = envelope['message']
        logger.info(f'Message: {pubsub_message}')

        attributes = {}
        if isinstance(pubsub_message, dict) and 'attributes' in pubsub_message:
            attributes = (
                base64.b64decode(pubsub_message['attributes']).decode('utf-8').strip()
            )
        elif 'attributes' in envelope:
            attributes = envelope['attributes']

        func = attributes.get('function')
        logger.info(f'Function: {func}')
        logger.info(f'Attributes: {attributes}')

        if func == 'gcp':
            gcp(attributes, None)
        elif func == 'hail':
            hail(attributes, None)
        elif func == 'seqr':
            seqr(attributes, None)

        logger.info('Success!')
        return ('Success!', 204)
    except Exception as exp:  # pylint: disable=broad-except
        logger.error(exp)
        return ('Server Error', 500)


if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host='0.0.0.0', port=PORT, debug=True)
