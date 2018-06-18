Basic Consume Example
=====================

This sample demonstrates how to establish a channel connection to the DXL
streaming service. Once the connection is established, the sample
repeatedly consumes and displays available records for the consumer group.

Prerequisites
*************

* A DXL streaming service is available for the sample to connect to.
* Credentials for a consumer are available for use with the sample.

Setup
*****

Modify the example to include the appropriate settings for the consumer
service channel:

    .. code-block:: python

        CHANNEL_URL = "http://127.0.0.1:50080"
        CHANNEL_USERNAME = "me"
        CHANNEL_PASSWORD = "secret"
        CHANNEL_CONSUMER_GROUP = "sample_consumer_group"
        CHANNEL_TOPIC_SUBSCRIPTIONS = ["case-mgmt-events"]
        # Path to a CA bundle file containing certificates of trusted CAs. The CA
        # bundle is used to validate that the certificate of the server being connected
        # to was signed by a valid authority. If set to an empty string, the server
        # certificate is not validated.
        VERIFY_CERTIFICATE_BUNDLE = ""


For testing purposes, you can use the ``fake_streaming_service`` Python tool
embedded in the OpenDXL Streaming Client SDK to start up a local
streaming service which includes some fake data for a single preconfigured
consumer group. The initial settings in the example above include the URL,
credentials, and consumer group used by the ``fake_streaming_service``.

To launch the ``fake_streaming_service`` tool, run the following command in
a command window:

    .. code-block:: shell

        python sample/fake_streaming_service.py

Messages like the following should appear in the command window:

    .. code-block:: shell

        INFO:__main__:Starting service
        INFO:__main__:Started service on http://mycaseserver:50080

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
should be displayed to the output window. Using the ``fake_streaming_service``,
for example, initial payloads similar to the following should appear:

    .. code-block:: shell

        2018-05-30 17:35:36,754 __main__ - INFO - Received payloads:
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
                "user": "johndoe"
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

        2018-05-30 17:39:27,895 __main__ - INFO - Received records:
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
                                      verify_cert_bundle=VERIFY_CERTIFICATE_BUNDLE),
                     consumer_group=CHANNEL_CONSUMER_GROUP,
                     verify_cert_bundle=VERIFY_CERTIFICATE_BUNDLE) as channel:

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


The first step is to create a channel to the streaming service. The channel
includes the URL to the streaming service, ``CHANNEL_URL``, and credentials
that the client uses to authenticate itself to the service, ``CHANNEL_USERNAME``
and ``CHANNEL_PASSWORD``.

The example defines a ``process_callback`` function which is invoked with the
payloads (a list of dictionary objects) extracted from records consumed from the
channel. The ``process_callback`` function outputs the contents of the
payloads parameter and returns ``True`` to indicate that the channel should
continue consuming records. Note that if the ``process_callback`` function were
to instead return ``False``, the ``run`` method would stop polling the service
for new records and would instead return.

The final step is to call the ``run`` method. The ``run`` method establishes a
consumer instance with the service, subscribes the consumer instance for events
delivered to the ``topics`` included in the ``CHANNEL_TOPIC_SUBSCRIPTIONS``
variable, and continuously polls the streaming service for available records.
The payloads from any records which are received from the streaming service are
passed in a call to the ``process_callback`` function. Note that if no records
are received from a poll attempt, an empty list of payloads is passed into the
``process_callback`` function.
