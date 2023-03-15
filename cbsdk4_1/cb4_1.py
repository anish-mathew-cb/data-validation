import json
import requests
from datetime import timedelta
from flask import Flask, request, jsonify

import cb_connector as cb4_1

# get a reference to our cluster

app = Flask(__name__)


# sync_admin_url = 'http://localhost:4985/org_chat'
sync_admin_url = 'https://zj6efasieqvxfprc.apps.cloud.couchbase.com:4985/org_chat_sync'



@app.route('/')
def hello_world():
    return 'Hello World'


@app.route('/set_bucket/<type>/<bucket>', methods = ['POST'])
def set_bucket(type,bucket):
    cb4_1.CBConnector412.set_dest(type.upper()) 
    cb4_1.CBConnector412.set_bucket(bucket)
    return 'set_bucket'

@app.route('/set_collection/<type>/<bucket>/<scope>/<collection>', methods = ['POST'])
@app.route('/set_collection/<type>/<bucket>//', methods = ['POST'], defaults={'scope': "", "collection": ""})
def set_collection(type, bucket, scope, collection):
    print(type.upper())
    cb4_1.CBConnector412.set_dest(type.upper()) 
    cb4_1.CBConnector412.set_bucket(bucket)
    cb4_1.CBConnector412.set_collection(scope, collection)
    return 'scope, collection'


@app.route('/get_doc/<type>/<id>')
def get_doc(type, id):
    doc = {}
    cb4_1.CBConnector412.set_dest(type.upper()) 
    doc = cb4_1.CBConnector412.get_doc(id)
    # print(json.dumps(doc), flush=True)
    return json.dumps(doc)


@app.route('/init', methods = ['POST'])
def init():
    try:
        nodes = request.json["nodes"]  # type: ignore
        username = request.json["username"]  # type: ignore
        password = request.json["password"]  # type: ignore
        secured = request.json["secured"]  # type: ignore
        # print(request.json, flush=True)
        # print("SRC" if request.json["type"] is None else request.json["type"].upper(), flush=True) # type: ignore
        cb4_1.CBConnector412.set_dest("SRC" if request.json["type"] is None else request.json["type"].upper() ) # type: ignore
        cb_connector = cb4_1.CBConnector412.init(nodes, username, password, secured )
        # print(data.get("nodes"), data.get("username"), data.get("password"), data.get("secured"), flush=True)
        return "SUCCESS"
    except Exception as e:
        print(e, flush=True)
        return "ERROR"



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
    # app.run(host='localhost', port=8080)


# , cert=("./org-chat-sync.pem")
