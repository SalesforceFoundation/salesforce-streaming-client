from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()


version = '0.1.4'

install_requires = [
    # List your project dependencies here.
    # For more details, see:
    # http://packages.python.org/distribute/setuptools.html#declaring-dependencies
    'salesforce-requests-oauthlib',
    'python-bayeux',
    'six'
]


setup(name='salesforce-streaming-client',
    version=version,
    description="A python client for the Salesforce Streaming API.",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords='salesforce cometd bayeux',
    author='Adam J. Lincoln',
    author_email='alincoln@salesforce.com',
    url='https://github.com/SalesforceFoundation/salesforce-streaming-client',
    download_url='https://github.com/SalesforceFoundation/salesforce-streaming-client/tarball/0.1.4',
    license='BSD 3-Clause',
    packages=find_packages('src'),
    package_dir = {'': 'src'},include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points={
    }
)
