'''
    Copyright (c) 2016, Salesforce.org
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Salesforce.org nor the names of
      its contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
'''

import os
import simplejson as json
import gevent.queue
import errno
from glob import glob
from random import choice
from string import ascii_lowercase
from datetime import datetime
from datetime import timedelta
from copy import deepcopy

from salesforce_requests_oauthlib import SalesforceOAuth2Session
from python_bayeux import BayeuxClient

from six.moves.urllib.parse import urlencode

import logging
LOG = logging.getLogger('salesforce_streaming_client')

# See https://docs.cometd.org/current/reference/#_bayeux for bayeux reference

default_settings_path = \
    os.path.expanduser('~/.salesforce_streaming_client')

default_replay_data_filename = 'replay_data.json'

streaming_endpoint = '/cometd/{0}/'


def _encode_set(v):
    if isinstance(v, set):
        return list(v)
    return v


def _decode_set(initial_result):
    # We can't modify the dict while we loop
    to_return = {}
    for k, v in initial_result.items():
        new_value = v

        if isinstance(v, list):
            new_value = set(v)

        if k.isdigit():
            to_return[int(k)] = new_value
        else:
            to_return[k] = new_value

    return to_return


def iso_to_datetime(iso_string):
    return datetime.strptime(
        iso_string.rsplit('.', 1)[0].rsplit('Z', 1)[0],
        '%Y-%m-%dT%H:%M:%S'
    )


class SalesforceStreamingClient(BayeuxClient):
    def __init__(self, oauth_client_id, client_secret, username,
                 settings_path=None,
                 sandbox=False,
                 local_server_settings=('localhost', 60443),
                 password=None,
                 ignore_cached_refresh_tokens=False,
                 version=None,
                 replay_client_id=None,
                 cleanup_datadir=True):

        self.cleanup_datadir = cleanup_datadir

        self.settings_path = \
            default_settings_path if settings_path is None else settings_path

        if not os.path.exists(self.settings_path):
            try:
                os.makedirs(self.settings_path)
            except OSError as e:  # Guard against race condition
                if e.errno != errno.EEXIST:
                    raise e

        if replay_client_id is None:
            replay_client_id = \
                ''.join(choice(ascii_lowercase) for i in range(12))

        self.replay_data_filename = os.path.join(
            self.settings_path,
            '{0}_{1}'.format(
                replay_client_id,
                default_replay_data_filename
            )
        )

        self.streaming_callbacks = {}

        self.replay_data = {}
        try:
            with open(self.replay_data_filename) as fileh:
                self.replay_data = json.load(fileh, object_hook=_decode_set)
        except IOError:
            pass

        self.created_streaming_channels = set([])

        oauth_session = SalesforceOAuth2Session(
            oauth_client_id,
            client_secret,
            username,
            settings_path=settings_path,
            sandbox=sandbox,
            local_server_settings=local_server_settings,
            password=password,
            ignore_cached_refresh_tokens=ignore_cached_refresh_tokens,
            version=version
        )

        super(SalesforceStreamingClient, self).__init__(
            streaming_endpoint.format(oauth_session.version),
            oauth_session,
            start=False
        )

    def handshake(self):
        return super(SalesforceStreamingClient, self).handshake(
            ext={
                'replay': True
            }
        )

    def publish(self, channel, message, keep_trying=False, autocreate=False):
        self.publication_queue.put({
            'channel': channel,
            'keep_trying': keep_trying,
            'autocreate': autocreate,
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
                publication = self.publication_queue.get(timeout=0.5)
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
                    if publication['autocreate']:
                        self.create_streaming_channel(channel)
                    if publication['autocreate'] or publication['keep_trying']:
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

    def generic_callback(self, connect_response_element):
        channel = connect_response_element['channel']
        event_data = connect_response_element['data']['event']
        this_replay_id = event_data['replayId']

        if channel not in self.replay_data:
            self.replay_data[channel] = {}

        if this_replay_id not in self.replay_data[channel]:
            # Push topics have a createdDate under event, but platform events
            # have a CreatedDate in the payload.  Instead of bringing type
            # through to the callback, we just find one.
            created_date = None
            if 'createdDate' in event_data:
                created_date = event_data['createdDate']
            else:
                payload = connect_response_element['data']['payload']
                created_date = payload['CreatedDate']
            self.replay_data[channel][this_replay_id] = {
                'created_date': created_date,
                'callbacks': set([])
            }

        callbacks_for_this_replay_id = \
            self.replay_data[channel][this_replay_id]['callbacks']
        for callback in self.streaming_callbacks[channel]:
            if callback not in callbacks_for_this_replay_id:
                getattr(self, callback)(connect_response_element)
                self.replay_data[channel][this_replay_id]['callbacks'].add(
                    callback
                )

        # Clean up old replay events for this channel
        day_ago = datetime.utcnow() - timedelta(1)  # Now minus one day
        # Allows us to del inside loop
        for replay_id, data in list(self.replay_data[channel].items()):
            if iso_to_datetime(data['created_date']) < day_ago:
                del self.replay_data[channel][replay_id]

    def subscribe(self, channel, callback, replay=True, autocreate=True):
        type = None
        if channel.startswith('/u/'):
            type = 'generic'
        elif channel.startswith('/topic/'):
            type = 'push_topic'
        elif channel.startswith('/event/'):
            type = 'event'
        else:
            raise BadSubscriptionException(
                '{0} is not a valid subscription channel.'.format(
                    channel
                )
            )

        if replay is True:
            # Get what we missed since we were last running
            if channel in self.replay_data:
                sorted_keys = sorted(self.replay_data[channel].keys())
                if len(sorted_keys) > 0:
                    replay = sorted_keys[-1]
                else:
                    replay = '-1'
            else:
                replay = '-1'
        elif replay is False:
            replay = '-1'
        elif replay == 'all':
            replay = '-2'
        # else we use whatever was passed

        if channel not in self.streaming_callbacks:
            self.streaming_callbacks[channel] = []
            super(SalesforceStreamingClient, self).subscribe(
                channel,
                callback='generic_callback',
                replay=replay,
                autocreate=autocreate,
                type=type
            )

        self.streaming_callbacks[channel].append(callback)

    # Fully overridden
    def _resubscribe(self):
        current_callbacks = deepcopy(self.streaming_callbacks)

        # Clear out BayeuxClient's subscriptions so we can resubscribe
        self.subscription_callbacks.clear()

        self.streaming_callbacks.clear()
        for channel, callbacks in current_callbacks.items():
            for callback in callbacks:
                self.subscribe(channel, callback)

    # Fully overridden
    def _subscribe_greenlet(self):
        channel = None
        replay = None
        autocreate = None
        type = None

        while True:
            subscription_queue_message = None
            LOG.info(
                'Client id {0} is looking for a new subscription'.format(
                    self.client_id
                )
            )
            try:
                subscription_queue_message = self.subscription_queue.get(
                    timeout=1
                )
                type = subscription_queue_message['type']
            except gevent.queue.Empty:
                if self.stop_greenlets:
                    break
                else:
                    continue

            LOG.info('Client id {0} found subscription info: {1}'.format(
                self.client_id,
                str(subscription_queue_message)
            ))
            channel = subscription_queue_message['channel']
            replay = subscription_queue_message['replay']
            autocreate = subscription_queue_message['autocreate']

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
                        channel: int(replay)
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

                        self.create_streaming_channel(channel)

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

    def create_streaming_channel(self, channel, delete_on_exit=True):
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

        if not duplicate and delete_on_exit:
            self.created_streaming_channels.add(
                create_streaming_channel_result.json()['id']
            )

    def _query_for_streaming_channel(self, channel):
        query_response = self.oauth_session.get(
            '/services/data/vXX.X/query/?{query}'.format(
                query=urlencode({
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

        day_ago = datetime.utcnow() - timedelta(1)  # Now minus one day
        for channel, replays in self.replay_data.items():
            self.replay_data[channel] = {
                replay_id:
                    data
                    for replay_id, data
                    in replays.items()
                    if iso_to_datetime(data['created_date']) >= day_ago
            }
        try:
            with open(self.replay_data_filename, 'w') as fileh:
                json.dump(self.replay_data, fileh, default=_encode_set)
        except IOError:
            pass

        # Clean up any old replay files we can find that have all old events
        # This could collide with reading/writing by other instances of this
        # client.
        if self.cleanup_datadir:
            for filename in glob(os.path.join(
                self.settings_path,
                '*{0}'.format(default_replay_data_filename)
            )):
                files_to_delete = []
                try:
                    with open(filename, 'r') as fileh:
                        file_replays = json.load(
                            fileh,
                            object_hook=_decode_set
                        )
                        delete_file = True
                        for file_replay in file_replays.values():
                            for data in file_replay.values():
                                if iso_to_datetime(data['created_date']) >= \
                                        day_ago:

                                    delete_file = False
                                    break
                            if not delete_file:
                                break
                        if delete_file:
                            files_to_delete.append(filename)
                except IOError:
                    pass
                for filename in files_to_delete:
                    os.remove(filename)


class BadSubscriptionException(Exception):
    def __init__(self, message):
        super(BadSubscriptionException, self).__init__(message)
