# needed for any cluster connection
import json
import hashlib
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta

import yaml

# needed to support SQL++ (N1QL) query
from couchbase.cluster import QueryOptions

timeout_options=ClusterTimeoutOptions(connect_timeout=timedelta(seconds=15), kv_timeout=timedelta(seconds=5), 
                                      query_timeout=timedelta(seconds=10))

final_result = []

def load_config_from_yaml(filename):
    config = {}
    with open(f'{filename}.yaml','r') as f: 
        config = yaml.safe_load(f)
    print('done!') 
    return config

def load_config_from_json(filename):
    config = {}
    with open(f'{filename}.json','r') as f: 
        config = json.load(f)
    print('done!') 
    return config

def get_colls_docs(bucket):
    doc_set_bucket = []
    for scope in bucket["scopes"]:
        for coll in scope["collections"]:
            doc_set_coll = {}
            doc_set_coll["bucket"] = bucket["name"]
            doc_set_coll["scope"]= scope["name"]
            doc_set_coll["collection"] = coll["name"]
            doc_set_coll["documents"] = coll["documents"]
            doc_set_bucket.append(doc_set_coll)
    # print(json.dumps(doc_set_bucket, indent=2))
    return doc_set_bucket 

def get_doc(coll, key):
    try:
        result = coll.get(key)
        return result.content_as[dict]
    except Exception as e:
        print(e)

def compare(bucket):
    documents = get_colls_docs(bucket)

    # Open the "default" bucket on both clusters
    for coll in documents:
        src_bucket = source.bucket(coll["bucket"])
        tgt_bucket = target.bucket(coll["bucket"])

        src_coll = src_bucket.scope(coll["scope"]).collection(coll["collection"])
        tgt_coll = tgt_bucket.scope(coll["scope"]).collection(coll["collection"])
        for doc in coll["documents"]:
            
            src_doc = get_doc(src_coll, doc)
            tgt_doc = get_doc(tgt_coll, doc)

            src_hash_json = json.dumps(src_doc, sort_keys=True)
            tgt_hash_json = json.dumps(tgt_doc, sort_keys=True)

            src_hash = hashlib.md5(src_hash_json.encode("utf-8")).hexdigest()
            tgt_hash = hashlib.md5(tgt_hash_json.encode("utf-8")).hexdigest()
            
            if src_hash != tgt_hash:
                result = {}
                result["bucket"] = coll["bucket"]
                result["scope"] = coll["bucket"]
                result["collection"] = coll["bucket"]
                result["doc_id"] = doc      
                result["src_doc"] = json.loads(src_hash_json)      
                result["tgt_doc"] = json.loads(tgt_hash_json)      

                print("src_hash_json", (src_hash_json))
                print("tgt_hash_json", (tgt_hash_json))

                print("result", json.dumps(result))
                final_result.append(result)

try: 
    # Load config from yaml
    config = load_config_from_yaml("./properties")

    # Connect to the first cluster
    print('couchbases://{0}'.format(",".join(config["source"]["nodes"])), config["source"]["username"], config["source"]["password"])
    src_mode = 'couchbases://{}' if config["source"]["secured"] else 'couchbase://{}'

    source = Cluster(src_mode.format(",".join(config["source"]["nodes"])), 
        ClusterOptions(PasswordAuthenticator(config["source"]["username"], config["source"]["password"]), 
            timeout_options=timeout_options)) # type: ignore

    # Connect to the second cluster
    print('couchbases://{0}'.format(",".join(config["target"]["nodes"])), config["target"]["username"], config["target"]["password"])
    tgt_mode = 'couchbases://{}' if config["target"]["secured"] else 'couchbase://{}'

    target = Cluster(tgt_mode.format(",".join(config["target"]["nodes"])), 
        ClusterOptions(PasswordAuthenticator(config["target"]["username"], config["target"]["password"]), 
            timeout_options=timeout_options)) # type: ignore

    for bucket in config["source"]["buckets"]:
        print("="*50)
        print("BUCKET: ", bucket["name"])
        compare(bucket)

    with open('final_result.txt', 'w') as file:
        json.dump(final_result, file, indent=2)

except Exception as e:
    print(e)