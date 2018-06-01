from __future__ import absolute_import
import json
import os
import signal
import sys
import time

from dxlstreamingconsumerclient.channel import Channel, ChannelAuth, ConsumerError

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
CHANNEL_URL = "http://127.0.0.1:50000"
CHANNEL_USERNAME = "me"
CHANNEL_PASSWORD = "secret"
CHANNEL_CONSUMER_GROUP = "mcafee_investigator_events"
#  Path to a CA bundle file containing certificates of trusted CAs. The CA
#  bundle is used to validate that the certificate of the server being connected
#  to was signed by a valid authority. If set to an empty string, the server
#  certificate is not validated.
VERIFY_CERTIFICATE_BUNDLE = ""

# This controls the amount of time that the sample waits between attempts
# to consume records from the consumer service.
WAIT_BETWEEN_QUERIES = 5

# Create a new channel object
with Channel(CHANNEL_URL,
             auth=ChannelAuth(CHANNEL_URL,
                              CHANNEL_USERNAME,
                              CHANNEL_PASSWORD,
                              verify=VERIFY_CERTIFICATE_BUNDLE),
             consumer_group=CHANNEL_CONSUMER_GROUP,
             verify=VERIFY_CERTIFICATE_BUNDLE) as channel:
    # Register a signal handler to be invoked when a user interrupts the
    # running sample (for example, by pressing CTRL-C)
    def signal_handler(*_):
        channel.retry_on_fail = False

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Starting event loop")
    while channel.retry_on_fail:
        # Create a new consumer on the consumer group provided when the channel
        # was created above.
        channel.create()

        # Subscribe the consumer to a list of topics. Since no explicit topics are
        # provided, this defaults to the 'case-mgmt-events' and 'BusinessEvents'
        # topics.
        channel.subscribe()

        consumer_error = False
        while not consumer_error and channel.retry_on_fail:
            try:
                # Repeatedly consume records from the subscribed topics - until
                # any errors or process interruptions occur.
                records = channel.consume()
                logger.info("Consumed records: \n%s",
                            json.dumps(records, indent=4, sort_keys=True))
                # Commit the offsets for the records which were just consumed.
                channel.commit()
                time.sleep(WAIT_BETWEEN_QUERIES)
            except ConsumerError as exp:
                # This exception could be raised if the consumer has been
                # removed. If the sample process has not been interrupted,
                # a new consumer will be created and the attempt to consume
                # records for the consumer will be repeated.
                logger.error("Resetting consumer loop: %s", exp)
                consumer_error = True
