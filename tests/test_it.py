from getpass import getpass
from pytest import fixture
from salesforce_streaming_client import SalesforceStreamingClient
import simplejson as json
import random
from string import ascii_letters
import urllib
from salesforce_requests_oauthlib import SalesforceOAuth2Session
import gevent
import logging


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

    _check_oauth(oauth_client_id, client_secret, username1, sandbox)
    _check_oauth(oauth_client_id, client_secret, username2, sandbox)

    return (
        oauth_client_id,
        client_secret,
        username1,
        username2,
        sandbox
    )


def _check_oauth(oauth_client_id, client_secret, username, sandbox):
    for i in xrange(2):  # Make two attempts
        try:
            SalesforceOAuth2Session(
                oauth_client_id,
                client_secret,
                username,
                sandbox=sandbox
            )
        except IOError as e:
            # This probably means that the local server certs aren't ready
            if e.args[1] == 'No such file or directory':
                assert False, \
                       'Please set up OAuth2 for your users with \n' \
                       '$ python -c \'from salesforce_requests_oauthlib ' \
                       'import SalesforceOAuth2Session; ' \
                       'SalesforceOAuth2Session.' \
                       'generate_local_webserver_key()\''


class ClientOne(SalesforceStreamingClient):
    def shutdown_myself(self, connect_response_element):
        connect_response_element_payload = \
            connect_response_element['data']['payload']
        self.result = connect_response_element_payload
        self.shutdown()


def test_one_client(caplog, get_oauth_info):
    caplog.setLevel(logging.INFO)

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
        try:
            streaming_client.block()
        except KeyboardInterrupt:
            assert False
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
            query=urllib.urlencode({
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
                 sandbox=False):
        self.random_value = random_value

        super(ClientIgnoreSelf, self).__init__(
            client_id,
            client_secret,
            username,
            sandbox=sandbox
        )

    def subscribe(self, channel, callback, replay=-1, autocreate=True):
        if not hasattr(self, 'real_callbacks'):
            self.real_callbacks = {}
        if channel not in self.real_callbacks:
            self.real_callbacks[channel] = []
        self.real_callbacks[channel].append(callback)

        super(ClientIgnoreSelf, self).subscribe(
            channel,
            'generic_callback',
            replay,
            autocreate
        )

    def generic_callback(self, connect_response_element):
        connect_response_element_payload = \
            json.loads(connect_response_element['data']['payload'])
        sending_client = connect_response_element_payload['sending_client']
        channel = connect_response_element['channel']
        if self.client_id != sending_client and \
           channel in self.real_callbacks:

            for callback in self.real_callbacks[channel]:
                getattr(self, callback)(connect_response_element)

    def publish(self, channel, message):
        super(ClientIgnoreSelf, self).publish(
            channel,
            json.dumps({
                'sending_client': self.client_id,
                'message': message
            }),
            keep_trying=True
        )

    def handler(self, connect_response_element):
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


def test_two_clients(caplog, get_oauth_info):
    caplog.setLevel(logging.INFO)

    random_value = ''.join(random.choice(ascii_letters) for i in range(12))

    random_streaming_channel_name = '/u/{0}'.format(
        ''.join(random.choice(ascii_letters) for i in range(10))
    )

    oauth_client_id = get_oauth_info[0]
    client_secret = get_oauth_info[1]
    username = get_oauth_info[2]
    username2 = get_oauth_info[3]
    sandbox = get_oauth_info[4]

    with ClientIgnoreSelf(oauth_client_id, client_secret,
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

        with ClientIgnoreSelf(oauth_client_id, client_secret, username2,
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
