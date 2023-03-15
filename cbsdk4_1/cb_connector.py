# needed for any cluster connection
import json
import hashlib
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions)  # type: ignore
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta

import yaml

# needed to support SQL++ (N1QL) query
from couchbase.cluster import QueryOptions


class CBConnector412:

    source = {
        "nodes"  : None,
        "username"  : None,
        "password"  : None,
        "secured"  : None,
        "bucket" : None,
        "cluster" : None,
        "scope" : None,
        "collection" : None
    }

    target = {
        "nodes"  : None,
        "username"  : None,
        "password"  : None,
        "secured"  : None,
        "bucket" : None,
        "cluster" : None,
        "scope" : None,
        "collection" : None
    }
    dest = source

    @classmethod
    def init(cls, nodes, username, password, secured):
        if cls.dest is None:
            cls.set_dest("SRC")

        cls.dest["nodes"] = nodes
        cls.dest["username"] = username
        cls.dest["password"] = password
        cls.dest["secured"] = secured

        try: 
            timeout_options=ClusterTimeoutOptions(connect_timeout=timedelta(seconds=15), kv_timeout=timedelta(seconds=5), 
                                            query_timeout=timedelta(seconds=10))
            mode = 'couchbases://{}' if cls.dest["secured"] is True else 'couchbase://{}'
            print(mode.format(",".join(cls.dest["nodes"])), cls.dest["username"], cls.dest["password"])

            cls.dest["cluster"] = Cluster(mode.format(",".join(cls.dest["nodes"])),  # type: ignore
                ClusterOptions(PasswordAuthenticator(cls.dest["username"], cls.dest["password"]), timeout_options=timeout_options)) # type: ignore
            print(json.dumps(cls.dest["nodes"]))
        except Exception as e:
            print(e)

    @classmethod
    def set_dest(cls, type):
        # print("TYPE", type)
        cls.dest = cls.source if type == "SRC" else cls.target

    @classmethod
    def set_bucket ( cls, bucket):
        try: 
            cls.dest["bucket"] = cls.dest["cluster"].bucket(bucket) # type: ignore
        except Exception as e:
            print(e)

    @classmethod
    def set_collection (cls, scope, collection):
        try: 
            if scope.replace(" ","") != "" and collection.replace(" ","") != "":
                cls.dest["collection"] = cls.dest["bucket"].scope(scope).collection(collection) # type: ignore
            else:
                cls.dest["collection"] = cls.dest["bucket"].default_collection() # type: ignore
        except Exception as e:
            print(e)

    @classmethod
    def get_doc(cls, key): # type: ignore
        try:
            result = cls.dest["collection"].get(key) # type: ignore
            return result.content_as[dict]
        except Exception as e:
            print(e)
