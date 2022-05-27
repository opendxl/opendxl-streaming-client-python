from __future__ import absolute_import
import json
import os
import sys
from dxlstreamingclient.channel import Channel, ChannelAuth, ClientCredentialsChannelAuth
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
CHANNEL_IAM_URL = "https://iam.server-cloud.com/"
CHANNEL_CLIENT_ID = "me"
CHANNEL_CLIENT_SECRET = "secret"
CHANNEL_CONSUMER_GROUP = "sample_consumer_group"
CHANNEL_TOPIC_SUBSCRIPTIONS = ["testTopic"]
CHANNEL_SCOPE=""
CHANNEL_GRANT_TYPE=""
CHANNEL_AUDIENCE=""
# Path to a CA bundle file containing certificates of trusted CAs. The CA
# bundle is used to validate that the certificate of the server being connected
# to was signed by a valid authority. If set to an empty string, the server
# certificate is not validated.
VERIFY_CERTIFICATE_BUNDLE = ""
# This constant controls the frequency (in seconds) at which the channel 'run'
# call below polls the streaming service for new records.
WAIT_BETWEEN_QUERIES = 5
# Create a new channel object
with Channel(CHANNEL_URL,
             auth=ClientCredentialsChannelAuth(CHANNEL_IAM_URL,
                              CHANNEL_CLIENT_ID,
                              CHANNEL_CLIENT_SECRET,
                              verify_cert_bundle=VERIFY_CERTIFICATE_BUNDLE,
                              scope=CHANNEL_SCOPE,
                              grand_type=CHANNEL_GRANT_TYPE,
                              audience=CHANNEL_AUDIENCE),
             consumer_group=CHANNEL_CONSUMER_GROUP,
             verify_cert_bundle=VERIFY_CERTIFICATE_BUNDLE
             ) as channel:
    # Create a function which will be called back upon by the 'run' method (see
    # below) when records are received from the channel.
    def process_callback(payloads):
        # Print the payloads which were received. 'payloads' is a list of
        # dictionary objects extracted from the records received from the
        # channel.
        logger.info("Received payloads: \n%s",
                    json.dumps(payloads, indent=4, sort_keys=True))
        # Return 'True' in order for the 'run' call to continue attempting to
        # consume records.
        return True
    # Consume records indefinitely
    channel.run(process_callback, wait_between_queries=WAIT_BETWEEN_QUERIES,
                topics=CHANNEL_TOPIC_SUBSCRIPTIONS)