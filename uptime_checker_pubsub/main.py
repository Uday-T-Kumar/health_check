#!/usr/bin/env python3
import avro.schema as schema
import bittensor
import configparser
import datetime
import functions_framework
import io
import json
import logging
import os
import requests
import time
from avro.io import BinaryEncoder, DatumWriter
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import firestore
from google.cloud.pubsub import PublisherClient
from google.pubsub_v1.types import Encoding
from requests.exceptions import HTTPError

#logging
logging.basicConfig(
    level='INFO',
    format='%(asctime)s - %(levelname)s - %(message)s',
    )
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging,'DEBUG'))

publisher_client = PublisherClient()


# Module to run curl requests on IP:Port and report back if they are reachable or not
def check_miner_ip(final_url):
    result = False
    try:
        logger.info("module start: check miner ip" )
        response = requests.get(final_url, timeout=0.4) #0.5,1,2
        logger.info(response)
        logger.info(response.status_code == 200)
        result = True
        logger.info("1 - 200 - Success")
    except HTTPError as http_err:              # Handling the HTTP errors
        if http_err.response.status_code ==404:
            logger.warning("1a - 404 - URL Didn't work")
            result = True                        # Marking them as responsive because it still points to a server
        elif http_err.response.status_code ==500:
            logger.warning("1b - Server error")
            result = False
    except requests.exceptions.Timeout:        # Handling timeouts
        logger.warning("2- The request timed out!")
        result = False
    except requests.exceptions.ConnectionError as e:
        if e.args and isinstance(e.args[0], tuple) and isinstance(e.args[0][1], ConnectionRefusedError):
            errno = e.args[0][1].errno
            logger.eror(f"3a -ConnectionError: Errno = {errno}")
            result = False
        else:
            logger.error(f"3b -ConnectionError: {e}")
            result = False
    except requests.exceptions.RequestException as e:  #Handling other request errors
        logger.error(f"4 -An error occurred: {e}")
        result = False
    except Exception as e:
        logger.error(f"5- Unexpected error {e}")
        result = False
    return result

#def pubsub_subs(topic_path, bout, record):
def pubsub_subs(topic_path, bout, record):
    try:
        # Get the topic encoding type.
        topic = publisher_client.get_topic(request={"topic": topic_path})
        encoding = topic.schema_settings.encoding

        # Encode the data according to the message serialization type.
        if encoding == Encoding.BINARY:
            encoder = BinaryEncoder(bout)
            writer.write(record, encoder)
            data = bout.getvalue()
            logger.debug(f"Preparing a binary-encoded message:\n{data.decode()}")
        elif encoding == Encoding.JSON:
            data_str = json.dumps(record)
            logger.debug(f"Preparing a JSON-encoded message:\n{data_str}")
            data = data_str.encode("utf-8")
        else:
            logger.debug(f"No encoding specified in {topic_path}. Abort.")
            exit(0)

        future = publisher_client.publish(topic_path, data)
        logger.info(f"Published message ID: {future.result()}")
    except NotFound:
        logger.debug(f"{topic_id} not found.")

### TO BE ENABLED BEFORE CONTAINERIZATION)
@functions_framework.http
def uptime_handler(request):
#if __name__ == '__main__':  #to be used for local testing only
    """
    Main Cloud Function entry point for metagraph fetching.
    """
    try:
        # Only allow GET requests (from Cloud Scheduler)
        if request.method != "GET":
            print("not working")
            exit()
            return (
                json.dumps({"error": "Method not allowed"}),
                405,
                {"Content-Type": "application/json"},
            )

        #logging
        #logging.basicConfig(
        #    level='INFO',
        #    format='%(asctime)s - %(levelname)s - %(message)s',
        #)
        #logger = logging.getLogger(__name__)
        #logger.setLevel(getattr(logging,'DEBUG'))

        start_time = time.perf_counter()
        local_time = time.localtime()
        formatted_time = time.strftime("%H:%M:%S", local_time)
        logger.info(f"Start time:{formatted_time}, {start_time:.6f} seconds")

        #logger.setLevel(getattr(logging,'DEBUG'))

        # print("***** Miner Uptime check start ******")
        logger.info("-----*****  Miner Uptime check start  ******-----")
        logger.info(f"Start time:{formatted_time}, {start_time:.6f} seconds")

        project_id='ni-sn27-frontend-dev'
        topic_id='Health_check_test'
        avsc_file='/home/udaytkumar_consult/uptime_checker_pubsub/health_check_schema.avsc'
        publisher_client = PublisherClient()
        topic_path1 = publisher_client.topic_path(project_id, topic_id)

        # Prepare to write Avro records to the binary output stream.
        with open(avsc_file, "rb") as file:
            avro_schema = schema.parse(file.read())
        writer = DatumWriter(avro_schema)
        bout = io.BytesIO()

        # Use the data from metagraf, check their connectivity & report to gcloud PubSub
        try:
            db = firestore.Client(
                project=project_id,
                database='validator-token-gateway-cache'
        )
            #print(db)
            logger.debug(db)
            if not db:
                    logging.error(
                        json.dumps({
                            "error": "Failed to initialize Firestore client",
                            "status": "error",
                            #"environment": config['environment']
                        }),
                        500,
                        {"Content-Type": "application/json"},
                    )
        except Exception as e:
            logging.error(f"Failed to initialize Firestore client: {e}")
            return (
                    json.dumps({
                        "error": f"Failed to connect to Firestore:: {str(e)}",
                        "status": "error"
                   }),
                   500,
                   {"Content-Type": "application/json"},
            )

        try:
            # Query for the latest document
            latest_doc_query = db.collection('uptime_metagraf').order_by(
                 'timestamp', direction=firestore.Query.DESCENDING
            ).limit(1)

            docs = latest_doc_query.get()
            # print(f"Full docs: {docs}\n\n")
            logger.debug(f"Full docs: {docs}\n\n")
            record_count = 0

            if docs:
                for doc in docs:
                    data=doc.to_dict()
                    axon = data["axon"]
                    detl = axon.values()

                    for dtl1 in detl:
                        ip = dtl1["ip"]
                        print(f"IP: {ip}")
                        logger.debug(f"IP: {ip}")
                        port = dtl1["port"]
                        print(f"port: {port}")
                        logger.debug(f"port: {port}")
                        hkey = dtl1["hotkey"]
                        print(f"hotkey: {hkey}")
                        logger.debug(f"hotkey: {hkey}")
                        final_url = "http://"+ip+":"+str(port)
                        print(f"final URL: {final_url}\n\n")
                        logger.info(f"final URL: {final_url}\n\n")

                        resp=check_miner_ip(final_url)  #use the final URL to 
                        #invoke function to make a curl request on IP:port; 
                        #Response is in the form of true/false
                        
                        logger.info(resp)
                        reachable = resp
                        current_dt = datetime.now(timezone.utc)
                        current_datetime = current_dt.strftime("%Y-%m-%d, %H:%M:%S")

                        # Prepare some data using a Python dictionary that matches the Avro schema
                        record = {"Time": current_datetime, "ID": hkey, "Reachable": reachable}
                        pubsub_subs(topic_path1, bout, record)
                        record_count+=1

                    result = {
                            "status": "success",
                            "message": "Details fetched from Firestore, uptime checked & published in Pubsub",
                            "topic_path": topic_path1,
                            "no of records": record_count,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    return (
                            json.dumps(result),
                            200,
                           {"Content-Type": "application/json"},
                    )

            else:
                logger.error("No documents found in the collection.")
                return (
                        json.dumps({
                            "error": "No metagraf documents found in Firestore",
                            "status": "error",
                            #"environment": config['environment']
                        }),
                        500,
                        {"Content-Type": "application/json"},
                )
        except Exception as e:
            logger.error(f"Failed to retreive metagraph data in Firestore: {e}")
            return (
                    json.dumps({
                        "error": f"Failed to retreive metagraph data in Firestore:: {str(e)}",
                        "status": "error"
                   }),
                   500,
                   {"Content-Type": "application/json"},
            )
    
        # Record the end time
        end_time = time.perf_counter()

        # Calculate the elapsed time
        elapsed_time = end_time - start_time

        # Log/Print the execution time
        print(f"Execution time: {elapsed_time:.6f} seconds")
        logger.info(f"Execution time: {elapsed_time:.6f} seconds")

    except Exception as e:
        logging.error(f"Uptime handler error: {str(e)}")
        return (
            json.dumps({
                "error": f"Internal server error: {str(e)}",
                "status": "error"
            }),
            500,
            {"Content-Type": "application/json"},
        )
