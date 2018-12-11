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
import json
import gevent.queue
import errno
from glob import glob
from random import choice
from string import ascii_lowercase
from datetime import datetime
from datetime import timedelta
from copy import deepcopy
from abc import ABCMeta
from abc import abstractmethod
import six
from six.moves.urllib.parse import urlencode
import pytz
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extras import Json
from psycopg2.extensions import AsIs

import requests
from salesforce_requests_oauthlib import SalesforceOAuth2Session
from salesforce_requests_oauthlib import HiddenLocalStorage as OauthlibHiddenLocalStorage
from salesforce_requests_oauthlib import PostgresStorage as OauthlibPostgresStorage
from python_bayeux import BayeuxClient
from python_bayeux import RepeatedTimeoutException


import logging
LOG = logging.getLogger('salesforce_streaming_client')

# See https://docs.cometd.org/current/reference/#_bayeux for bayeux reference

default_settings_path = \
    os.path.expanduser('~/.salesforce_streaming_client')

default_replay_data_filename = 'replay_data.json'

streaming_endpoint = '/cometd/{0}/'


def _encode_types(v):
    if isinstance(v, set):
        return list(v)
    elif isinstance(v, datetime):
        return v.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
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


def string_to_datetime(date_string):
    return datetime.strptime(
        date_string,
        '%Y-%m-%dT%H:%M:%S.%fZ'
    ).replace(tzinfo=pytz.utc)


@six.add_metaclass(ABCMeta)
class ReplayDataStorageMechanism:
    @abstractmethod
    def store(self, tokens):
        pass

    @abstractmethod
    def retrieve(self):
        pass

    @abstractmethod
    def clean(self, replay_data, lower_boundary):
        pass


class HiddenLocalStorage(ReplayDataStorageMechanism):
    def __init__(self, replay_client_id, settings_path=default_settings_path):
        if not os.path.exists(settings_path):
            try:
                os.makedirs(settings_path)
            except OSError as e:  # Guard against race condition
                if e.errno != errno.EEXIST:
                    raise e

        self.full_settings_path = os.path.join(
            settings_path,
            '{0}_{1}'.format(
                replay_client_id,
                default_replay_data_filename
            )
        )

        self.oauth_token_storage = OauthlibHiddenLocalStorage()

    def store(self, replay_data):
        # Yes, overwrite
        with open(self.full_settings_path, 'w') as fileh:
            json.dump(replay_data, fileh, default=_encode_types)

    def retrieve(self):
        replay_data = {}
        try:
            with open(self.full_settings_path, 'r') as fileh:
                replay_data = json.load(fileh, object_hook=_decode_set)
        except IOError:
            return replay_data
        else:
            for channel, replay_id_data in replay_data.items():
                for replay_id, data in replay_id_data.items():
                    data['created_date'] = \
                            string_to_datetime(data['created_date'])

        return replay_data

    def clean(self, replay_data, lower_boundary):
        # Clean up any old replay files we can find that have all old events
        # This could collide with reading/writing by other instances of this
        # client.
        files_to_delete = []

        for filename in glob(os.path.join(
            os.path.dirname(self.full_settings_path),
            '*_{0}'.format(default_replay_data_filename)
        )):
            try:
                with open(filename, 'r') as fileh:
                    file_contents = fileh.read()
            except IOError:
                pass

            if len(file_contents) == 0:
                files_to_delete.append(filename)
                continue

            file_replays = json.loads(
                file_contents,
                object_hook=_decode_set
            )
            delete_file = True
            for file_replay in file_replays.values():
                for data in file_replay.values():
                    if string_to_datetime(
                        data['created_date']
                    ) >= lower_boundary:
                        delete_file = False
                        break

                if not delete_file:
                    break
            if delete_file:
                files_to_delete.append(filename)

        for filename in files_to_delete:
            os.remove(filename)


class PostgresStorage(ReplayDataStorageMechanism):
    def __init__(
        self,
        replay_client_id,
        database_uri=None,
        schema_name='salesforce_streaming_client'
    ):
        self.replay_client_id = replay_client_id

        self.oauth_token_storage = OauthlibPostgresStorage(
            database_uri
        )

        if database_uri is None:
            database_uri = os.environ['DATABASE_URL']

        self.table_name = 'replay_data'
        self.schema_name = schema_name

        with psycopg2.connect(database_uri, sslmode='require') as pg_conn:
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(
                'SELECT COUNT(*) FROM information_schema.schemata '
                'WHERE schema_name = %s',
                (self.schema_name,)
            )
            schema_count = pg_cursor.fetchone()[0]

            if schema_count == 0:
                pg_cursor.execute(
                    'CREATE SCHEMA %s',
                    (AsIs(self.schema_name),)
                )
                pg_conn.commit()

            pg_cursor.execute(
                'SET search_path TO %s',
                (AsIs(self.schema_name),)
            )

            pg_cursor.execute(
                'SELECT COUNT(*) '
                'FROM information_schema.tables '
                'WHERE table_schema = %s '
                'AND table_name = %s '
                'AND table_type = %s',
                (self.schema_name, self.table_name, 'BASE TABLE')
            )
            table_count = pg_cursor.fetchone()[0]
            if table_count == 0:
                create_table_template = '''CREATE TABLE %s (
    replay_client_id text not null,
    channel text not null,
    replay_id integer not null,
    created_date timestamp with time zone not null,
    callbacks jsonb,
    primary key (replay_client_id, channel, replay_id)
)'''
                pg_cursor.execute(
                    create_table_template,
                    (AsIs(self.table_name),)
                )

        self.database_uri = database_uri

    def store(self, replay_data):
        with psycopg2.connect(self.database_uri, sslmode='require') as pg_conn:
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(
                'SET search_path TO %s',
                (AsIs(self.schema_name),)
            )
            insert_stmt = '{0} %s ON CONFLICT ' \
                          '(replay_client_id, channel, replay_id) DO UPDATE '\
                          'SET created_date = EXCLUDED.created_date, ' \
                          'callbacks = EXCLUDED.callbacks'
            insert_stmt = insert_stmt.format(
                pg_cursor.mogrify(
                    'INSERT INTO %s (replay_client_id, channel, replay_id, ' \
                    'created_date, callbacks) VALUES',
                    (AsIs(self.table_name),)
                ).decode()
            )

            values = []
            for channel, replay_id_data in replay_data.items():
                for replay_id, replay_data in replay_id_data.items():
                    values.append((
                        self.replay_client_id,
                        channel,
                        replay_id,
                        replay_data['created_date'],
                        Json(_encode_types(replay_data['callbacks']))
                    ))

            execute_values(
                pg_cursor,
                insert_stmt,
                values
            )

    def retrieve(self):
        to_return = {}

        # We'll reconnect every time, because it might be a long time between
        # DB access
        with psycopg2.connect(self.database_uri, sslmode='require') as pg_conn:
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(
                'SET search_path TO %s',
                (AsIs(self.schema_name),)
            )
            pg_cursor.execute(
                'SELECT channel, replay_id, created_date, ' \
                'callbacks FROM %s WHERE replay_client_id = %s',
                (AsIs(self.table_name), self.replay_client_id)
            )

            for result in pg_cursor.fetchall():
                if result[0] not in to_return:
                    to_return[result[0]] = {}
                channel_data = to_return[result[0]]

                channel_data[result[1]] = {
                    'created_date': result[2],
                    'callbacks': result[3]
                }

        return to_return

    def clean(self, replay_data, lower_boundary):
        # Clean up any rows with old created dates
        with psycopg2.connect(self.database_uri, sslmode='require') as pg_conn:
            pg_cursor = pg_conn.cursor()
            pg_cursor.execute(
                'SET search_path TO %s',
                (AsIs(self.schema_name),)
            )

            delete_template = 'DELETE FROM %s ' \
                'WHERE created_date < %s'
            pg_cursor.execute(
                delete_template,
                (AsIs(self.table_name), lower_boundary)
            )


class SalesforceStreamingClient(BayeuxClient):
    def __init__(self, oauth_client_id, client_secret, username,
                 sandbox=False,
                 local_server_settings=None,
                 callback_settings=None,
                 password=None,
                 ignore_cached_refresh_tokens=False,
                 version=None,
                 replay_client_id=None,
                 clean_replay_data=True,
                 replay_data_storage=None,
                 **kwargs):

        self.clean_replay_data = clean_replay_data

        # for backward compatibility
        self.callback_settings = callback_settings
        if self.callback_settings is None:
            if local_server_settings is None:
                self.callback_settings = ('localhost', 60443)
            else:
                self.callback_settings = local_server_settings

        if replay_client_id is None:
            replay_client_id = \
                ''.join(choice(ascii_lowercase) for i in range(12))

        if replay_data_storage is None:
            replay_data_storage = HiddenLocalStorage(replay_client_id)

        if isinstance(replay_data_storage, ReplayDataStorageMechanism):
            self.replay_data_storage = replay_data_storage
        else:
            self.replay_data_storage = replay_data_storage(replay_client_id)

        self.streaming_callbacks = {}

        self.replay_data = self.replay_data_storage.retrieve()

        self.created_streaming_channels = set([])

        oauth_session = SalesforceOAuth2Session(
            oauth_client_id,
            client_secret,
            username,
            sandbox=sandbox,
            callback_settings=callback_settings,
            password=password,
            ignore_cached_refresh_tokens=ignore_cached_refresh_tokens,
            version=version,
            token_storage=self.replay_data_storage.oauth_token_storage,
            **kwargs
        )
        if version is None:
            oauth_session.use_latest_version()

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
                        if not self.shutdown_called:
                            self.publication_queue.put(publication)
                            gevent.sleep(1)
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
                'created_date': string_to_datetime(created_date),
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
        day_ago = datetime.now(tz=pytz.utc) - timedelta(1)  # Now minus one day
        # Allows us to del inside loop
        for replay_id, data in list(self.replay_data[channel].items()):
            if data['created_date'] < day_ago:
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
        self.waiting_for_resubscribe = True

        current_callbacks = deepcopy(self.streaming_callbacks)

        # Clear out BayeuxClient's subscriptions so we can resubscribe
        self.subscription_callbacks.clear()

        self.streaming_callbacks.clear()
        for channel, callbacks in current_callbacks.items():
            for callback in callbacks:
                self.subscribe(channel, callback)

        self.waiting_for_resubscribe = False

    # Fully overridden
    def _subscribe_greenlet(self, successive_timeout_threshold=20,
                            timeout_wait=5):
        channel = None
        replay = None
        autocreate = None
        type = None

        successive_timeouts = 0
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

            subscribe_responses = []
            try:
                subscribe_responses = self._send_message(
                    subscribe_request_payload
                )
            except requests.exceptions.ReadTimeout:
                successive_timeouts += 1

                if successive_timeouts > successive_timeout_threshold:
                    raise RepeatedTimeoutException('subscribe')

                gevent.sleep(timeout_wait)
                self.subscription_queue.put(subscription_queue_message)
            else:
                successive_timeouts = 0

            for subscribe_response in subscribe_responses:
                if not subscribe_response['successful']:
                    error = subscribe_response['error']
                    if error == '404::Unknown channel' and \
                       type == 'generic' and \
                       autocreate:

                        self.create_streaming_channel(channel)

                        # Toss it back into the queue
                        self.subscription_queue.put(subscription_queue_message)

                    elif error == '402::Unknown client' or error == \
                                  '403::Unknown client':

                        # Just try again, and eventually connect() will re-try
                        # a handshake
                        self.subscription_queue.put(subscription_queue_message)

                    elif subscribe_response['error'] == \
                            '403::Organization total events daily limit exceeded':

                        raise BadSubscriptionException(
                            'Could not subscribe to {0} because "{1}"'
                            .format(
                                channel,
                                subscribe_response['error']
                            )
                        )
                    else:
                        # Yes, this does mean that failures may not look quite
                        # right if the response contains more than one element.
                        raise BadSubscriptionException(
                            '{0} does not exist and was not autocreated. '
                            'Error: {1}'.format(
                                channel,
                                error
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

        day_ago = datetime.now(tz=pytz.utc) - timedelta(1)  # Now minus one day
        for channel, replays in self.replay_data.items():
            self.replay_data[channel] = {
                replay_id:
                    data
                    for replay_id, data
                    in replays.items()
                    if data['created_date'] >= day_ago
            }

        self.replay_data_storage.store(self.replay_data)

        if self.clean_replay_data:
            self.replay_data_storage.clean(self.replay_data, day_ago)


class BadSubscriptionException(Exception):
    def __init__(self, message):
        super(BadSubscriptionException, self).__init__(message)
