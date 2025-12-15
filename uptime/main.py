#!/usr/bin/env python3
"""
Metagraph Fetcher Cloud Function.

This Cloud Function runs the metagraph-uptime-dev.py script logic directly.
It's designed to be triggered by Cloud Scheduler on an hourly basis.
"""

import json
import logging
import os
from datetime import datetime, timezone

import bittensor
import functions_framework
from google.cloud import firestore

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_environment_config():
    """
    Get environment-specific configuration based on function name.
    
    Returns:
        dict: Configuration with project_id, network, netuid, and collection
    """
    function_name = os.environ.get('K_SERVICE', '')
    
    if 'production' in function_name.lower():
        return {
            'project_id': 'ni-sn27-frontend-prod',
            'network': 'finney',
            'netuid': 27,
            'collection': 'uptime_metagraf',
            'environment': 'production'
        }
    else:
        return {
            'project_id': 'ni-sn27-frontend-dev',
            'network': 'test',
            'netuid': 15,
            'collection': 'uptime_metagraf',
            'environment': 'development'
        }


def get_firestore_client(project_id):
    """
    Initializes and returns a Firestore client.
    """
    try:
        db = firestore.Client(
            project=project_id,
            database='validator-token-gateway-cache'
        )
        return db
    except Exception as e:
        logging.error(f"Failed to initialize Firestore client: {e}")
        return None


def fetch_metagraph(network, netuid):
    """
    Fetches the metagraph from the Bittensor network.
    """
    try:
        logging.info(f"Connecting to Bittensor network '{network}' for netuid {netuid}...")
        subtensor = bittensor.subtensor(network=network)
        metagraph = bittensor.metagraph(netuid=netuid, subtensor=subtensor)
        metagraph.sync()
        logging.info("Successfully fetched metagraph.")
        return metagraph
    except Exception as e:
        logging.error(f"Failed to fetch metagraph: {e}")
        return None


def store_metagraph_data(db, metagraph, collection, network, netuid, environment):
    """
    Stores the metagraph data in Firestore.
    """
    if not db or not metagraph:
        return False

    timestamp = datetime.now(timezone.utc)
    doc_id = f"{environment}_{timestamp.isoformat()}"
    doc_ref = db.collection(collection).document(doc_id)

    """
    neurons_map = {}
    for uid in range(len(metagraph.hotkeys)):
        hotkey = metagraph.hotkeys[uid]
        neuron_info = {
            "uid": uid,
            "hotkey": metagraph.hotkeys[uid],
            "ip": float(metagraph.ip[uid]),
            "port": float(metagraph.port[uid]),
            "consensus": float(metagraph.C[uid]),
        }
        neurons_map[hotkey] = neuron_info
    """
    axon_map = {}
    for uid, ax in enumerate(metagraph.axons):
        hotkey = metagraph.hotkeys[uid]
        axon_info = {
            "uid": uid,
            "hotkey": metagraph.hotkeys[uid],
            "ip": ax.ip,
            "port": ax.port,
        }
        axon_map[hotkey] = axon_info

    snapshot_data = {
        "timestamp": timestamp,
        "network": network,
        "netuid": netuid,
        "environment": environment,
        "axon_count": len(axon_map),
        "axon": axon_map,
    }

    try:
        logging.info(f"Storing metagraph snapshot {doc_id} in Firestore...")
        doc_ref.set(snapshot_data)
        logging.info("Successfully stored metagraph data.")
        return True
    except Exception as e:
        logging.error(f"Failed to store metagraph data in Firestore: {e}")
        return False


@functions_framework.http
def metagraph_handler(request):
    """
    Main Cloud Function entry point for metagraph fetching.
    """
    try:
        # Only allow GET requests (from Cloud Scheduler)
        if request.method != "GET":
            return (
                json.dumps({"error": "Method not allowed"}),
                405,
                {"Content-Type": "application/json"},
            )

        # Get environment configuration
        config = get_environment_config()
        
        logging.info(f"Starting metagraph fetch for {config['environment']} environment")

        # Initialize Firestore client
        db = get_firestore_client(config['project_id'])
        if not db:
            return (
                json.dumps({
                    "error": "Failed to initialize Firestore client",
                    "status": "error",
                    "environment": config['environment']
                }),
                500,
                {"Content-Type": "application/json"},
            )

        # Fetch metagraph
        metagraph = fetch_metagraph(config['network'], config['netuid'])
        if not metagraph:
            return (
                json.dumps({
                    "error": "Failed to fetch metagraph",
                    "status": "error",
                    "environment": config['environment']
                }),
                500,
                {"Content-Type": "application/json"},
            )

        # Store metagraph data
        success = store_metagraph_data(
            db, 
            metagraph, 
            config['collection'], 
            config['network'], 
            config['netuid'],
            config['environment']
        )

        if success:
            result = {
                "status": "success",
                "message": "Metagraph data fetched and stored successfully",
                "environment": config['environment'],
                "network": config['network'],
                "netuid": config['netuid'],
                "neuron_count": len(metagraph.hotkeys),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            return (
                json.dumps(result),
                200,
                {"Content-Type": "application/json"},
            )
        else:
            return (
                json.dumps({
                    "error": "Failed to store metagraph data",
                    "status": "error",
                    "environment": config['environment']
                }),
                500,
                {"Content-Type": "application/json"},
            )

    except Exception as e:
        logging.error(f"Metagraph handler error: {str(e)}")
        return (
            json.dumps({
                "error": f"Internal server error: {str(e)}",
                "status": "error"
            }),
            500,
            {"Content-Type": "application/json"},
        )

