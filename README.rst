salesforce-streaming-client
==========================

A Salesforce streaming API client for python, built on salesforce-requests-oauthlib and python-bayeux.


Tests
-----

To run tests, install py.test and pytest-cov in your virtualenv and

$ py.test --cov=src/salesforce_streaming_client/ --cov-report html:coverage

View test coverage results at ``./coverage``.

Note that the tests will complain if you haven't set up a self-signed cert for the web application oauth2 flow.
It will ask you to do this:

``python -c 'from salesforce_requests_oauthlib import SalesforceOAuth2Session; SalesforceOAuth2Session.generate_local_webserver_key()'``

Credits
-------

- `Distribute`_
- `modern-package-template`_
- `py.test`_
- `pytest-cov`_

.. _Distribute: http://pypi.python.org/pypi/distribute
.. _`modern-package-template`: http://pypi.python.org/pypi/modern-package-template
.. _`py.test`: http://doc.pytest.org/en/latest/index.html
.. _`pytest-cov`: https://pypi.python.org/pypi/pytest-cov
