salesforce-streaming-client
===========================

A Salesforce streaming API client for python, built on salesforce-requests-oauthlib and python-bayeux.


Please note: as of the 1.0.0 release of python-bayeux (a dependency of salesforce-streaming-client), code using this
library will need to call gevent's monkey patching mechanisms.  It may have previously been possible to rely on
python-bayeux's internal call, but that is no longer supported.

Also, as of the 1.0.0 release of python-bayeux, it no longer officially supports python 2.  It may be possible to continue
using this library with python 2 but it is not supported.


Tests
-----

To run tests, install py.test and pytest-cov in your virtualenv and

$ py.test --cov=src/salesforce_streaming_client/ --cov-report html:coverage

View test coverage results at ``./coverage``.

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
