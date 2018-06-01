Basic Consume Example
=====================

This sample demonstrates how to establish a channel connection to the DXL
streaming consumer service. Once the connection is established, the sample
repeatedly consumes and displays available records for the consumer group.

Prerequisites
*************
* A DXL streaming consumer service is available for the sample to connect to.
* Credentials for a consumer are available for use with the sample.

Setup
*****

Modify the example to include the appropriate settings for the consumer
service channel:

    .. code-block:: python

        CHANNEL_URL = "http://127.0.0.1:50000"
        CHANNEL_USERNAME = "me"
        CHANNEL_PASSWORD = "secret"
        CHANNEL_CONSUMER_GROUP = "mcafee_investigator_events"
        #  Path to a CA bundle file containing certificates of trusted CAs. The CA
        #  bundle is used to validate that the certificate of the server being connected
        #  to was signed by a valid authority. If set to an empty string, the server
        #  certificate is not validated.
        VERIFY_CERTIFICATE_BUNDLE = ""

For testing purposes, you can use the ``fake_consumer_service`` Python tool
embedded in the OpenDXL Streaming Consumer Client SDK to start up a local
consumer service which includes some fake data for a single preconfigured
consumer group. The initial settings in the example above include the url,
credentials, and consumer group used by the ``fake_consumer_service``.

To launch the ``fake_consumer_service`` tool, run the following command in
a command window:

    .. code-block:: shell

        python tools/fake_consumer_service.py

Messages like the following should appear in the command window:

    .. code-block:: shell

        INFO:__main__:Starting service
        INFO:__main__:Started service on 0.0.0.0:50000

Running
*******

To run this sample execute the ``sample/basic/basic_consume_example.py`` script
as follows:

    .. parsed-literal::

        python sample/basic/basic_consume_example.py

The initial line in the output window should be similar to the following:

    .. code-block:: shell

        2018-05-30 17:35:36,743 __main__ - INFO - Starting event loop

As records are received by the sample, the contents of the message payloads
should be displayed to the output window. Using the ``fake_consumer_service``,
for example, initial records similar to the following should appear:

    .. code-block:: shell

        2018-05-30 17:35:36,754 __main__ - INFO - Consumed records:
        [
            {
                "case": {
                    "id": "9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
                    "name": "A great case full of malware",
                    "priority": "Low",
                    "url": "https://mycaseserver.com/#/cases/9ab2cebb-6b5f-418b-a15f-df1a9ee213f2"
                },
                "entity": "case",
                "id": "a45a03de-5c3d-452a-8a37-f68be954e784",
                "nature": "",
                "origin": "",
                "tenant-id": "7af4746a-63be-45d8-9fb5-5f58bf909c25",
                "timestamp": "",
                "transaction-id": "",
                "type": "creation",
                "user": "jmdacruz"
            },
            {
                "case": {
                    "id": "9ab2cebb-6b5f-418b-a15f-df1a9ee213f2",
                    "name": "A great case full of malware",
                    "priority": "Low",
                    "url": "https://mycaseserver.com/#/cases/9ab2cebb-6b5f-418b-a15f-df1a9ee213f2"
                },
                "entity": "case",
                "id": "a45a03de-5c3d-452a-8a37-f68be954e784",
                "nature": "",
                "origin": "",
                "tenant-id": "7af4746a-63be-45d8-9fb5-5f58bf909c25",
                "timestamp": "",
                "transaction-id": "",
                "type": "priority-update",
                "user": "other"
            }
        ]

When no new records are available from the service, the sample should output
a line similar to the following:

    .. code-block:: shell

        2018-05-30 17:39:27,895 __main__ - INFO - Consumed records:
        []

Details
*******

The majority of the sample code is shown below:

    .. code-block:: python

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


After creating a channel, a signal handler is registered to receive signals
which are typically generated when the running process is about to be
terminated — for example, if the user were to press CTRL-C while the
example is running. When the signal handler fires, it sets the
``channel.retry_on_fail`` property to ``False``. This causes any operations
that the channel may be performing — for example, a ``channel.consume`` call
— to avoid triggering retries in the event of a failure. When the signal
handler fires, the process should be terminated as soon as possible.

The next step is to call the ``create`` method on the channel. This creates a
consumer on the consumer service for the consumer group supplied when the
channel was first constructed.

The example calls the ``subscribe`` method in order to subscribe the
consumer to a list of topics.  Since no explicit topics are provided as an
argument to the method, this defaults to the "case-mgmt-events" and
"BusinessEvents" topics.

The example then repeatedly calls the ``consume`` method to consume the next
list of records available from the service, outputs the contents of the payloads
from those records to the console, and calls the ``commit`` method to commit
the offsets associated with those records back to the service. The ``commit``
operation allows the consumer offset to be advanced so that the next ``consume``
call returns the next list of records available from the service.

If a ``ConsumerError`` is raised for a ``consume`` or ``commit`` call, the
consumer may have been removed from the consumer group by the service. In this
case, the example falls back to calling the ``create`` and ``subscribe`` methods
in order to create a new consumer for the consumer group and re-establish
subscriptions for the new consumer before continuing to consume records.
