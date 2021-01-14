import constants as const
from flask import jsonify, request, make_response, Flask
import socket
from flasgger import Swagger
import logging
import logging.config
import datetime
import traceback
import sys
import os
import requests
PATH = os.path.dirname(os.path.abspath(__file__))
APP_PATH = os.path.split(PATH)[0]
sys.path.append(APP_PATH+"/src-lib/")
from measure import measure_time_with_path, measure_time_old


logging.config.fileConfig(const.LOGGING_CONFIG)
logger = logging.getLogger('logfile')
app = Flask(__name__)
from cache import models, model_encodings
app.config['models'] = models
app.config['model_encodings'] = model_encodings
import computeLTR as ltr
import computeUM as um

app.config['SWAGGER'] = {
    'swagger_version': '2.0',
    'specs': [
        {
            'version': '0.1alpha',
            'title': 'Cognitive Claims API (v1)',
            'description': 'Builds a API for Cognitive Claims models',
            'endpoint': 'v1_spec',
            'route': '/'
        }
    ]
}

Swagger(app)


@app.route('/processLTR', methods=['POST'])
@measure_time_with_path('/processLTR')
def process_ltr():
    """
        Post request for Cognitive Claims processing
        ---
        tags:
            - Cognitive Claims

        summary: 'processClaim'
        consumes:
            - application/json
        produces:
            - text/html
        parameters:
            - name: Claim_input
              in: body
              required: true
              description: JSON input for request
              schema:
                type: object
                properties:
                    header:
                        type: object
                    details:
                        type: "array"
                        items:
                            type: object
                    edits:
                        type: "array"
                        items:
                            type: object
        responses:
            200:
                description: Predictions
            500:
                description: Errors
    """
    resp = {}
    try:         
        payload = request.get_json(silent=True)
        logger.debug(payload)
        resp = ltr.process_request(payload)                        
    except:
        logger.error((''.join(map(str.rstrip, traceback.format_exception(*sys.exc_info())))).replace('\n', ' '))
        resp['error'] = str(sys.exc_info()[1]).replace('\n', ' ')
    finally:
        return make_response(str(resp), 200)
 

@app.route('/processUM', methods=['POST'])
@measure_time_with_path('/processUM')
def process_um():
    """
        Post request for Cognitive Claims processing
        ---
        tags:
            - Cognitive Claims

        summary: 'processClaim'
        consumes:
            - application/json
        produces:
            - text/html
        parameters:
            - name: Claim_input
              in: body
              required: true
              description: JSON input for request
              schema:
                type: object
                properties:
                    um_claim:
                        type: object
        responses:
            200:
                description: Predictions
            500:
                description: Errors
    """
    resp = {}
    try:         
        payload = request.get_json(silent=True)
        logger.debug(payload)
        resp = um.process_request(payload['um_claim'])                        
    except:
        logger.error((''.join(map(str.rstrip, traceback.format_exception(*sys.exc_info())))).replace('\n', ' '))
        resp['error'] = str(sys.exc_info()[1]).replace('\n', ' ')
    finally:
        return make_response(str(resp), 200)


if __name__ == '__main__':
    HOSTNAME = socket.gethostname()
    if HOSTNAME == 'LC02W10KYHTDD.':
        HOSTNAME = 'localhost'
    print(HOSTNAME)
    PORT = 9080
    app.run(host=HOSTNAME, port=PORT, debug=True)