import simplejson as json
import gevent.queue
import urllib
from salesforce_requests_oauthlib import SalesforceOAuth2Session
from python_bayeux import BayeuxClient

import logging
LOG = logging.getLogger('salesforce_streaming_client')

# See https://docs.cometd.org/current/reference/#_bayeux for bayeux reference

streaming_endpoint = '/cometd/{version}/'.format(
    version='37.0'
)


class SalesforceStreamingClient(BayeuxClient):
    def __init__(self, oauth_client_id, client_secret, username,
                 settings_path=None,
                 sandbox=False,
                 local_server_settings=('localhost', 60443),
                 password=None,
                 ignore_cached_refresh_tokens=False,
                 version=None):

        self.created_streaming_channels = set([])

        super(SalesforceStreamingClient, self).__init__(
            streaming_endpoint,
            SalesforceOAuth2Session(
                oauth_client_id,
                client_secret,
                username,
                settings_path=settings_path,
                sandbox=sandbox,
                local_server_settings=local_server_settings,
                password=password,
                ignore_cached_refresh_tokens=ignore_cached_refresh_tokens,
                version=version
            ),
            start=False
        )

    def handshake(self):
        return super(SalesforceStreamingClient, self).handshake(
            ext={
                'replay': True
            }
        )

    def publish(self, channel, message, keep_trying=False):
        self.publication_queue.put({
            'channel': channel,
            'keep_trying': keep_trying,
            'message': json.dumps({
                "pushEvents": [
                    {
                        "payload": message,
                        "userIds": []
                    }
                ]
            })
        })

    def _publish_greenlet(self):
        while True:
            publication = None
            try:
                publication = self.publication_queue.get(timeout=0.05)
            except gevent.queue.Empty:
                if self.stop_greenlets:
                    break
                else:
                    continue

            channel = publication['channel']

            channel_id = None
            if channel not in self.channel_ids:
                channel_id = self._query_for_streaming_channel(channel)
                if channel_id is None:
                    if publication['keep_trying']:
                        self.publication_queue.put(publication)
                        continue
                    else:
                        raise ValueError(
                            '"{0}" is not a StreamingChannel name in this org'
                            .format(
                                channel
                            )
                        )
                self.channel_ids[channel] = channel_id

            LOG.info(
                'Client id {0} publishing message {1} to channel '
                '{2} ({3})'.format(
                    self.client_id,
                    str(publication['message']),
                    channel,
                    self.channel_ids[channel]
                )
            )

            # Note that this return value isn't going anywhere.
            # TODO: at least check for failures
            publish_response = self.oauth_session.post(
                '/services/data/vXX.X/sobjects/StreamingChannel/{Id}/push'
                .format(
                    Id=self.channel_ids[channel]
                ),
                headers={
                    'Content-Type': 'application/json'
                },
                data=publication['message']
            )

            LOG.info('Publication response: {0}'.format(publish_response.text))

    def subscribe(self, channel, callback, replay=-1, autocreate=True):
        type = None
        if channel.startswith('/u/'):
            type = 'generic'
        elif channel.startswith('/topic/'):
            type = 'push_topic'
        else:
            raise BadSubscriptionException(
                '{0} is not a valid subscription channel.'.format(
                    channel
                )
            )

        super(SalesforceStreamingClient, self).subscribe(
            channel,
            callback=callback,
            replay=replay,
            autocreate=autocreate,
            type=type
        )

    # Fully overridden
    def _subscribe_greenlet(self):
        channel = None
        replay = None
        autocreate = None
        type = None

        while True:
            try:
                LOG.info(
                    'Client id {0} is looking for a new subscription'.format(
                        self.client_id
                    )
                )
                subscription_queue_message = self.subscription_queue.get(
                    timeout=0.05
                )
                LOG.info('Client id {0} found subscription info: {1}'.format(
                    self.client_id,
                    str(subscription_queue_message)
                ))
                channel = subscription_queue_message['channel']
                replay = subscription_queue_message['replay']
                autocreate = subscription_queue_message['autocreate']
                type = subscription_queue_message['type']
            except gevent.queue.Empty:
                if self.stop_greenlets:
                    break
                else:
                    continue

            subscribe_request_payload = {
                # MUST
                'channel': '/meta/subscribe',
                'subscription': channel,
                'clientId': None,
                # MAY
                'id': None,
                'ext': {
                    'replay': {
                        # -2 works as string or int, but -1 or other
                        # replay ids only work as int
                        channel: replay
                    }
                }
            }

            subscribe_responses = self._send_message(subscribe_request_payload)
            for subscribe_response in subscribe_responses:
                if not subscribe_response['successful']:
                    error = subscribe_response['error']
                    if error == '404::Unknown channel' and \
                       type == 'generic' and \
                       autocreate:

                        create_streaming_channel_result = \
                            self.oauth_session.post(
                                '/services/data/vXX.X/sobjects/'
                                'StreamingChannel',
                                data=json.dumps({
                                    'Name': channel,
                                    'Description':
                                        'Made by salesforce-streaming-client'
                                }),
                                headers={
                                    'Content-Type': 'application/json'
                                }
                            )
                        result_payload = create_streaming_channel_result.json()
                        LOG.info(
                            'Client id {0} made streaming channel {1} '
                            'with result payload: {2}'.format(
                                self.client_id,
                                channel,
                                str(result_payload)
                            )
                        )
                        # Check whether we lost a race.  Maybe we should just
                        # try to create every time if we have to check for the
                        # error anyway...
                        failure = None
                        duplicate = False
                        if isinstance(result_payload, list):
                            for element in result_payload:
                                if 'errorCode' in element:
                                    if element['errorCode'] == \
                                       'DUPLICATE_DEVELOPER_NAME':

                                        duplicate = True

                                    else:
                                        # This is something other than a lost
                                        # race so we care about it
                                        failure = element

                        if failure is not None:
                            raise BadSubscriptionException(
                                'StreamingChannel {0} creation failed: {1}'
                                .format(
                                    channel,
                                    str(failure)
                                )
                            )

                        if not duplicate:
                            self.created_streaming_channels.add(
                                create_streaming_channel_result.json()['id']
                            )

                        # Toss it back into the queue
                        self.subscription_queue.put(subscription_queue_message)
                    elif subscribe_response['error'] == '402::Unknown client':
                        # Just try again, and eventually connect() will re-try
                        # a handshake
                        self.subscription_queue.put(subscription_queue_message)
                    else:
                        # Yes, this does mean that failures may not look quite
                        # right if the response contains more than one element.
                        raise BadSubscriptionException(
                            '{0} does not exist and was not autocreated.'
                            .format(
                                channel
                            )
                        )

    def _query_for_streaming_channel(self, channel):
        query_response = self.oauth_session.get(
            '/services/data/vXX.X/query/?{query}'.format(
                query=urllib.urlencode({
                    'q': 'SELECT Id, Name '
                         'FROM StreamingChannel '
                         'WHERE Name = \'{0}\''.format(
                             channel
                         )
                })
            )
        )

        LOG.info('Streaming Channel Query Response: {0}'.format(
            query_response.text
        ))

        query_response_payload = query_response.json()

        # An error here comes back as a list
        if not isinstance(query_response_payload, list) and \
           query_response_payload['totalSize'] > 0:
            return query_response_payload['records'][0]['Id']
        else:
            return None

    def __exit__(self, exception_type, exception_value, traceback):
        super(SalesforceStreamingClient, self).__exit__(
            exception_type,
            exception_value,
            traceback
        )

        for streaming_channel_id in self.created_streaming_channels:
            self.oauth_session.delete(
                '/services/data/vXX.X/sobjects/StreamingChannel/{0}'.format(
                    streaming_channel_id
                )
            )


class BadSubscriptionException(Exception):
    def __init__(self, message):
        super(BadSubscriptionException, self).__init__(message)
