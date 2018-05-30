from __future__ import absolute_import
import json
import os
import signal
import sys
import time

from dxldbconsumerclient.channel import Channel, ChannelAuth, ConsumerError
from dxldbconsumerclient import globals

root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_dir + "/../..")
sys.path.append(root_dir + "/..")

# Import common logging and configuration
from common import *

# Configure local logger
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

CHANNEL_URL = "http://127.0.0.1:50000"
CHANNEL_USERNAME = "me"
CHANNEL_PASSWORD = "secret"
CHANNEL_CONSUMER_GROUP = "mcafee_investigator_events"
WAIT_BETWEEN_QUERIES = 5

def signal_handler(*_):
    globals.interrupted = True

signal.signal(signal.SIGINT, signal_handler)

channel = Channel(CHANNEL_URL,
                  auth=ChannelAuth(CHANNEL_URL,
                                   CHANNEL_USERNAME,
                                   CHANNEL_PASSWORD),
                  consumer_group=CHANNEL_CONSUMER_GROUP)

logger.info("Starting event loop")
while not globals.interrupted:
    # loop over the channel and wait for events
    channel.create()
    channel.subscribe()

    consumer_error = False
    while not consumer_error and not globals.interrupted:
        try:
            records = channel.consume()
            logger.info("Consumed records: \n%s",
                        json.dumps(records, indent=4, sort_keys=True))
            channel.commit()
            time.sleep(WAIT_BETWEEN_QUERIES)
        except ConsumerError as exp:
            logger.error("Resetting consumer loop: %s", exp)
            consumer_error = True

logger.info("Deleting consumer")
try:
    channel.delete()
except Exception as exp:
    logger.warning(
        "Error while attempting to stop consumer with ID '%s': %s",
        channel.consumer_id, exp)
