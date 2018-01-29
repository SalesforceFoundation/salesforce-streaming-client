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

from getpass import getpass
from pytest import fixture
from salesforce_streaming_client import SalesforceStreamingClient
from salesforce_streaming_client import _decode_set
from salesforce_streaming_client import _encode_set
import simplejson as json
import random
from string import ascii_letters
from salesforce_requests_oauthlib import SalesforceOAuth2Session
import gevent
import logging
from random import choice
from string import ascii_lowercase
from six.moves.urllib.parse import urlencode


@fixture(scope='module')
def get_oauth_info():
    # Yes, it's annoying that you can't see these that are not
    # secret echoed.  But getpass() is smart about where it opens the input
    # stream, so I'm using it for now.
    oauth_client_id = getpass(
        'Enter full path to a test config file, or '
        'enter an oauth2 client identifier: '
    )

    config_fileh = None
    try:
        config_fileh = open(oauth_client_id, 'r')
    except IOError:
        client_secret = getpass('Enter oauth2 client secret: ')
        username1 = getpass('Enter first username: ')
        username2 = getpass('Enter second username: ')
        sandbox = getpass('Enter yes if sandbox: ') == 'yes'
    else:
        lines = config_fileh.readlines()
        oauth_client_id = lines[0].rstrip()
        client_secret = lines[1].rstrip()
        username1 = lines[2].rstrip()
        username2 = lines[3].rstrip()
        sandbox = lines[4].rstrip() == 'yes'

    return (
        oauth_client_id,
        client_secret,
        username1,
        username2,
        sandbox
    )


class ClientOne(SalesforceStreamingClient):
    def shutdown_myself(self, connect_response_element):
        connect_response_element_payload = \
            connect_response_element['data']['payload']
        self.result = connect_response_element_payload
        self.shutdown()


def test_one_client(caplog, get_oauth_info):
    caplog.set_level(logging.INFO)

    random_value = ''.join(random.choice(ascii_letters) for i in range(12))

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    sandbox = get_oauth_info[4]

    with ClientOne(oauth_client_id, client_secret, username,
                   sandbox=sandbox) as streaming_client:
        streaming_client.subscribe(
            random_streaming_channel_name,
            'shutdown_myself'
        )
        streaming_client.start()
        streaming_client.publish(
            random_streaming_channel_name,
            random_value,
            keep_trying=True
        )

        streaming_client.block()

        assert streaming_client.result == random_value

    oauth_session = SalesforceOAuth2Session(
        oauth_client_id,
        client_secret,
        username,
        sandbox=sandbox
    )
    # Make sure we cleaned up the hanging streaming channel
    query_response = oauth_session.get(
        '/services/data/vXX.X/query/?{query}'.format(
            query=urlencode({
                'q': 'SELECT Id, Name '
                     'FROM StreamingChannel '
                     'WHERE Name = \'{0}\''.format(
                         random_streaming_channel_name
                     )
            })
        )
    )
    query_response_payload = query_response.json()

    # An error here comes back as a list
    assert not isinstance(query_response_payload, list) and \
        query_response_payload['totalSize'] == 0


class ClientIgnoreSelf(SalesforceStreamingClient):
    def __init__(self, client_id, client_secret, username, random_value=None,
                 sandbox=False, replay_client_id=None,
                 really_ignore_self=True):

        self.random_value = random_value
        self.really_ignore_self = really_ignore_self

        super(ClientIgnoreSelf, self).__init__(
            client_id,
            client_secret,
            username,
            sandbox=sandbox,
            replay_client_id=replay_client_id
        )

    def publish(self, channel, message, keep_trying=True, autocreate=False):
        super(ClientIgnoreSelf, self).publish(
            channel,
            json.dumps({
                'sending_client': self.client_id,
                'message': message
            }),
            keep_trying=keep_trying,
            autocreate=autocreate
        )

    def start_publish_loop(self, publish_channel, publish_message):
        self.publish_channel = publish_channel
        self.publish_message = publish_message
        self.loop_greenlet = gevent.Greenlet(self._publish_loop)
        self.greenlets.append(self.loop_greenlet)
        self.loop_greenlet.start()

    def _publish_loop(self):
        while not self.stop_greenlets:
            self.publish(
                self.publish_channel,
                self.publish_message
            )
            gevent.sleep(1)

    def handler(self, connect_response_element):
        connect_response_element_payload = \
            json.loads(connect_response_element['data']['payload'])

        sending_client = connect_response_element_payload['sending_client']

        if not self.really_ignore_self or self.client_id != sending_client:
            self.real_handler(connect_response_element)

    def real_handler(self, connect_response_element):
        # Override in child classes
        pass


class ClientForTwo(ClientIgnoreSelf):
    def real_handler(self, connect_response_element):
        connect_response_element_payload = \
            json.loads(connect_response_element['data']['payload'])

        message = connect_response_element_payload['message']
        if message == 'SYN':
            # Start sending ACK
            self.start_publish_loop(
                connect_response_element['channel'],
                'ACK'
            )
        elif message == 'ACK':
            # Stop sending SYN
            self.loop_greenlet.kill()
            # Start sending SYN-ACK
            self.start_publish_loop(
                connect_response_element['channel'],
                'SYN-ACK'
            )
        elif message == 'SYN-ACK':
            # Stop sending ACK
            self.loop_greenlet.kill()
            # Send the secret
            self.publish(
                connect_response_element['channel'],
                self.random_value
            )
            # Go away
            self.shutdown()
        else:
            # Stop sending SYN-ACK
            self.loop_greenlet.kill()
            self.received_random_value = \
                connect_response_element_payload['message']
            # Go away
            self.shutdown()


def test_two_clients(caplog, get_oauth_info):
    caplog.set_level(logging.INFO)

    random_value = ''.join(random.choice(ascii_letters) for i in range(12))

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    username2 = get_oauth_info[3]
    sandbox = get_oauth_info[4]

    with ClientForTwo(oauth_client_id, client_secret,
                      username, sandbox=sandbox) as streaming_client1:
        streaming_client1.subscribe(
            random_streaming_channel_name,
            'handler'
        )
        streaming_client1.start()
        streaming_client1.go()

        streaming_client1.start_publish_loop(
            random_streaming_channel_name,
            'SYN'
        )

        with ClientForTwo(oauth_client_id, client_secret, username2,
                          random_value=random_value,
                          sandbox=sandbox) as streaming_client2:
            streaming_client2.subscribe(
                random_streaming_channel_name,
                'handler'
            )
            streaming_client2.start()
            try:
                streaming_client2.block()
                # Already running, but wait until it stops
                streaming_client1.block()
            except KeyboardInterrupt:
                assert False
            assert streaming_client1.received_random_value == \
                streaming_client2.random_value


class ClientForReplay(ClientIgnoreSelf):
    def __init__(self, client_id, client_secret, username, random_value=None,
                 sandbox=False, replay_client_id=None,
                 really_ignore_self=True):
        self.counts_received = 0
        self.counts_sent = 0

        super(ClientForReplay, self).__init__(
            client_id,
            client_secret,
            username,
            sandbox=sandbox,
            replay_client_id=replay_client_id,
            really_ignore_self=really_ignore_self
        )

    def publish(self, channel, message, keep_trying=True, autocreate=False):
        if message.startswith('COUNT'):
            self.counts_sent += 1

        super(ClientForReplay, self).publish(
            channel,
            message,
            keep_trying=keep_trying,
            autocreate=autocreate
        )

    def another_handler(self, connect_response_element):
        self.handler(connect_response_element)

    def real_handler(self, connect_response_element):
        connect_response_element_payload = \
            json.loads(connect_response_element['data']['payload'])

        message = connect_response_element_payload['message']
        if message == 'SYN':
            # Send one ACK
            self.publish(
                connect_response_element['channel'],
                'ACK'
            )

        elif message == 'ACK':
            # Stop sending SYN
            self.loop_greenlet.kill()

            # Send one SYN-ACK
            self.publish(
                connect_response_element['channel'],
                'SYN-ACK'
            )

            self.start_publish_loop(
                connect_response_element['channel'],
                'COUNT'
            )
        elif message == 'SYN-ACK':
            self.publish(
                connect_response_element['channel'],
                'I am done'
            )

            self.shutdown()

            # Wait for 10 seconds.  The other client will be publishing
            # COUNTs
            gevent.sleep(10)

        elif message.startswith('COUNT'):
            self.counts_received += 1

        else:
            # Stop sending COUNTs if we are
            if hasattr(self, 'loop_greenlet') and self.loop_greenlet:
                self.loop_greenlet.kill()

            # Make sure the other client stops
            self.publish(
                connect_response_element['channel'],
                'Time to stop'
            )

            # Go away
            self.shutdown()


def test_publish_no_subscribe(caplog, get_oauth_info):
    caplog.set_level(logging.INFO)

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    sandbox = get_oauth_info[4]

    with ClientForReplay(oauth_client_id, client_secret,
                         username, sandbox=sandbox) as streaming_client1:
        streaming_client1.start()
        streaming_client1.go()

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT',
            keep_trying=False,
            autocreate=True
        )

        streaming_client1.shutdown()

        assert streaming_client1.counts_sent == 1


def test_replay_new_client(caplog, get_oauth_info):
    caplog.set_level(logging.INFO)

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    username2 = get_oauth_info[3]
    sandbox = get_oauth_info[4]

    with ClientForReplay(oauth_client_id, client_secret,
                         username, sandbox=sandbox) as streaming_client1:

        streaming_client1.create_streaming_channel(
            random_streaming_channel_name,
            delete_on_exit=False
        )

        streaming_client1.start()
        streaming_client1.go()

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT',
            keep_trying=False,
            autocreate=True
        )

        streaming_client1.shutdown()

    with ClientForReplay(oauth_client_id, client_secret, username2,
                         sandbox=sandbox) as streaming_client2:

        # So the client will clean up the channel when it exits
        streaming_client2.created_streaming_channels.add(
            random_streaming_channel_name
        )

        streaming_client2.subscribe(
            random_streaming_channel_name,
            'handler',
            replay='all'
        )
        streaming_client2.start()
        streaming_client2.go()

        while streaming_client2.counts_received == 0:
            gevent.sleep(0.05)

        streaming_client2.shutdown()

        assert streaming_client2.counts_received == 1


def test_replay_do_not_repeat_handler(caplog, get_oauth_info):
    caplog.set_level(logging.INFO)

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    username2 = get_oauth_info[3]
    sandbox = get_oauth_info[4]

    first_replay_id = None

    replay_client_id = ''.join(choice(ascii_lowercase) for i in range(12))

    with ClientForReplay(oauth_client_id, client_secret,
                         username, sandbox=sandbox,
                         replay_client_id=replay_client_id,
                         really_ignore_self=False) as streaming_client1:

        streaming_client1.create_streaming_channel(
            random_streaming_channel_name,
            delete_on_exit=False
        )

        streaming_client1.start()
        streaming_client1.go()

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT1'
        )

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT2'
        )

        streaming_client1.subscribe(
            random_streaming_channel_name,
            'handler',
            replay='all'
        )

        channel_not_in_replay_data = \
            random_streaming_channel_name not in streaming_client1.replay_data
        while \
            channel_not_in_replay_data or \
            len(streaming_client1.replay_data[random_streaming_channel_name]) \
                < 2:

            gevent.sleep(0.05)

            channel_not_in_replay_data = \
                random_streaming_channel_name not in \
                streaming_client1.replay_data

        replay_ids = sorted(
            streaming_client1.replay_data[random_streaming_channel_name].keys()
        )
        first_replay_id = replay_ids[0]

        streaming_client1.shutdown()

    with ClientForReplay(oauth_client_id, client_secret, username2,
                         replay_client_id=replay_client_id,
                         sandbox=sandbox) as streaming_client2:

        # So the client will clean up the channel when it exits
        streaming_client2.created_streaming_channels.add(
            random_streaming_channel_name
        )

        # We only want the second COUNT from client 1, but we are going to
        # ignore it because we have handled it already
        streaming_client2.subscribe(
            random_streaming_channel_name,
            'handler',
            replay=first_replay_id
        )
        streaming_client2.start()
        streaming_client2.go()

        # Make sure we wait a bit for the already handled message to come back
        gevent.sleep(2)

        streaming_client2.shutdown()

        assert streaming_client2.counts_received == 0


def test_replay_new_client_specific_replay_id(caplog, get_oauth_info):
    caplog.set_level(logging.INFO)

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    username2 = get_oauth_info[3]
    sandbox = get_oauth_info[4]

    first_replay_id = None
    second_replay_id = None

    replay_client_id = ''.join(choice(ascii_lowercase) for i in range(12))

    with ClientForReplay(oauth_client_id, client_secret,
                         username, sandbox=sandbox,
                         replay_client_id=replay_client_id,
                         really_ignore_self=False) as streaming_client1:

        streaming_client1.create_streaming_channel(
            random_streaming_channel_name,
            delete_on_exit=False
        )

        streaming_client1.start()
        streaming_client1.go()

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT1'
        )

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT2'
        )

        streaming_client1.subscribe(
            random_streaming_channel_name,
            'handler',
            replay='all'
        )

        channel_not_in_replay_data = \
            random_streaming_channel_name not in streaming_client1.replay_data
        while \
            channel_not_in_replay_data or \
            len(streaming_client1.replay_data[random_streaming_channel_name]) \
                < 2:

            gevent.sleep(0.05)

            channel_not_in_replay_data = \
                random_streaming_channel_name not in \
                streaming_client1.replay_data

        replay_ids = sorted(
            streaming_client1.replay_data[random_streaming_channel_name].keys()
        )
        first_replay_id = replay_ids[0]
        second_replay_id = replay_ids[1]

        streaming_client1.shutdown()

    with ClientForReplay(oauth_client_id, client_secret, username2,
                         replay_client_id=replay_client_id,
                         sandbox=sandbox) as streaming_client2:

        # This is kind of contrived, but we want to tell streaming_client2
        # to handle COUNT2 again
        del streaming_client2.replay_data[random_streaming_channel_name]

        # So the client will clean up the channel when it exits
        streaming_client2.created_streaming_channels.add(
            random_streaming_channel_name
        )

        # We only want the second COUNT from client 1
        streaming_client2.subscribe(
            random_streaming_channel_name,
            'handler',
            replay=first_replay_id
        )
        streaming_client2.start()
        streaming_client2.go()

        try:
            while streaming_client2.counts_received == 0:
                gevent.sleep(0.05)
        except:
            assert False

        streaming_client2.shutdown()

        assert streaming_client2.counts_received == 1
        new_replay_data_keys = list(
            streaming_client2.replay_data[random_streaming_channel_name].keys()
        )
        assert new_replay_data_keys == [second_replay_id]


def test_replay_new_client_default_replay(caplog, get_oauth_info):
    caplog.set_level(logging.INFO)

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    username2 = get_oauth_info[3]
    sandbox = get_oauth_info[4]

    replay_client_id = ''.join(choice(ascii_lowercase) for i in range(12))

    with ClientForReplay(oauth_client_id, client_secret,
                         username, sandbox=sandbox,
                         replay_client_id=replay_client_id,
                         really_ignore_self=False) as streaming_client1:

        streaming_client1.create_streaming_channel(
            random_streaming_channel_name,
            delete_on_exit=False
        )

        streaming_client1.start()
        streaming_client1.go()

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT1'
        )

        streaming_client1.publish(
            random_streaming_channel_name,
            'COUNT2'
        )

        streaming_client1.subscribe(
            random_streaming_channel_name,
            'handler',
            replay='all'
        )

        channel_not_in_replay_data = \
            random_streaming_channel_name not in streaming_client1.replay_data
        while \
            channel_not_in_replay_data or \
            len(streaming_client1.replay_data[random_streaming_channel_name]) \
                < 2:

            gevent.sleep(0.05)

            channel_not_in_replay_data = \
                random_streaming_channel_name not in \
                streaming_client1.replay_data

        streaming_client1.shutdown()

    # Edit the saved replay_data file to remove the highest replay id
    replay_data = {}
    with open(streaming_client1.replay_data_filename, 'r') as fileh:
        replay_data = json.load(fileh, object_hook=_decode_set)
    highest_replay_id = \
        sorted(list(replay_data[random_streaming_channel_name].keys()))[-1]
    del replay_data[random_streaming_channel_name][highest_replay_id]
    with open(streaming_client1.replay_data_filename, 'w') as fileh:
        json.dump(replay_data, fileh, default=_encode_set)

    with ClientForReplay(oauth_client_id, client_secret, username2,
                         replay_client_id=replay_client_id,
                         sandbox=sandbox) as streaming_client2:

        # So the client will clean up the channel when it exits
        streaming_client2.created_streaming_channels.add(
            random_streaming_channel_name
        )

        # We only want the second COUNT from client 1
        streaming_client2.subscribe(
            random_streaming_channel_name,
            'handler'
        )
        streaming_client2.start()
        streaming_client2.go()

        try:
            while streaming_client2.counts_received == 0:
                gevent.sleep(0.05)
        except:
            assert False

        streaming_client2.shutdown()

        assert streaming_client2.counts_received == 1
        new_replay_data_keys = \
            streaming_client2.replay_data[random_streaming_channel_name].keys()
        assert highest_replay_id in new_replay_data_keys
