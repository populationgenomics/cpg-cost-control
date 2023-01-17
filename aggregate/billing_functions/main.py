# pylint: disable=unused-import,missing-module-docstring
import os
import base64
from flask import Flask, request

from gcp import from_pubsub as gcp  # noqa: F401
from hail import from_pubsub as hail  # noqa: F401
from seqr import from_pubsub as seqr  # noqa: F401

app = Flask(__name__)


@app.route('/', methods=['POST'])
def index():
    """
    Entrypoint for the pubsub request for the Cloud Run job
    """

    try:
        envelope = request.get_json()
        if not envelope:
            msg = 'no Pub/Sub message received'
            print(f'error: {msg}')
            return f'Bad Request: {msg}', 400

        if not isinstance(envelope, dict) or 'message' not in envelope:
            msg = 'invalid Pub/Sub message format'
            print(f'error: {msg}')
            return f'Bad Request: {msg}', 400

        pubsub_message = envelope['message']

        attributes = {}
        if isinstance(pubsub_message, dict) and 'attributes' in pubsub_message:
            attributes = (
                base64.b64decode(pubsub_message['attributes']).decode('utf-8').strip()
            )
        elif 'attributes' in envelope:
            attributes = (
                base64.b64decode(envelope['attributes']).decode('utf-8').strip()
            )

        func = attributes.get('function')
        if func == 'gcp':
            gcp(attributes, None)
        elif func == 'hail':
            hail(attributes, None)
        elif func == 'seqr':
            seqr(attributes, None)

        return ('', 204)
    except Exception:  # pylint: disable=W0703
        return ('', 200)


if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host='0.0.0.0', port=PORT, debug=True)
