from __future__ import absolute_import
import base64
import json
import os
import sys

from dxlstreamingclient.channel import Channel, ChannelAuth

root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_dir + "/../..")
sys.path.append(root_dir + "/..")

# Import common logging and configuration
from common import *

# Configure local logger
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

# Change these below to match the appropriate details for your
# channel connection.
CHANNEL_URL = "http://127.0.0.1:50080"
CHANNEL_USERNAME = "me"
CHANNEL_PASSWORD = "secret"
CHANNEL_TOPIC = "my-topic"
# Path to a CA bundle file containing certificates of trusted CAs. The CA
# bundle is used to validate that the certificate of the server being connected
# to was signed by a valid authority. If set to an empty string, the server
# certificate is not validated.
VERIFY_CERTIFICATE_BUNDLE = ""

# Create the message payload to be included in a record
message_payload = {
    "message": "Hello from OpenDXL"
}

# Create the full payload with records to produce to the channel
channel_payload = {
    "records": [
        {
            "routingData": {
                "topic": CHANNEL_TOPIC,
                "shardingKey": ""
            },
            "message": {
                "headers": {},
                # Convert the message payload from a dictionary to a
                # base64-encoded string.
                "payload": base64.b64encode(
                    json.dumps(message_payload).encode()).decode()
            }
        }
    ]
}

# Create a new channel object
with Channel(CHANNEL_URL,
             auth=ChannelAuth(CHANNEL_URL,
                              CHANNEL_USERNAME,
                              CHANNEL_PASSWORD,
                              verify_cert_bundle=VERIFY_CERTIFICATE_BUNDLE),
             verify_cert_bundle=VERIFY_CERTIFICATE_BUNDLE) as channel:
    # Produce the payload records to the channel
    channel.produce(channel_payload)

print("Succeeded.")
