### Data Validation


This Python code is a script that performs a comparison of data between two Couchbase clusters. It uses the Python SDK for Couchbase to retrieve documents from each cluster and then compare the contents of these documents to see if they are different.

The script begins by importing the necessary libraries, including `json`, `hashlib`, `couchbase`, `yaml`, and `datetime`.

Next, there are two helper functions to load configuration information from either a YAML or JSON file.

`get_colls_docs()` is a function that returns an array of dictionaries for each collection in the specified bucket. Each dictionary contains the bucket, scope, collection name, and an array of documents.

`get_doc()` is a function that retrieves a document from a specified collection using the document ID.

`compare()` is the main function of the script, which takes in a bucket object, retrieves the collections and documents for that bucket, and then loops through each document in each collection, comparing the document contents between the source and target clusters. If a difference is found, a dictionary object is created and appended to the `final_result` array.

### Steps to Execute

1. Clone the code from this repository 
2. Edit the properties.yaml to add the connection strings and other details
3. There are 3 folders 
   1. cb4_1 - contains scripts which run on couchbase sdk 4.1.2
   2. cb3_2 - contains scripts which run on couchbase sdk 3.2.7
   3. processor - data comparinf script

4. There are requirements.txt in each of the folders mentioned above
5. Create virtual environments in each of the above mentioned folder
   1. cb4_1 - `virtualenv env4_1`
   2. cb3_2 - `virtualenv env3_2`
   3. processor - `virtualenv env`
6. Activate virtualenv in each  folder `source ./<env_name>/bin/activate`
7. Install dependencies in each folder - `pip install -r requirements.txt`
8. Invoke the apis from cb4_1 and cb3_2 folders - `flask --app app.py --debug run`


### Commands

Open one `sh` window

```
cd cb4_1
virtualenv env4_1
source ./env4_1/bin/activate
pip install -r requirements.txt
./env4_1/bin/flask --app app.py --debug run --port 8080
```

Open 2nd `sh` window

```
cd ../cb3_2
virtualenv env3_2
source ./env3_2/bin/activate
pip install -r requirements.txt
./env3_2/bin/flask --app app.py --debug run --port 8090 &
```

Open 3rd `sh` window

```
cd ../processor
virtualenv env
source ./env/bin/activate
pip install -r requirements.txt
python process.py
```


Finally, the script reads in configuration information from a YAML file, connects to both source and target Couchbase clusters using the Python SDK, and then iterates over each bucket in the source cluster to compare the documents between the two clusters. The results of the comparison are saved to a file named `final_result.txt`.

<!-- Note: The script could benefit from additional error handling and logging to make it more robust and informative in case of errors. -->