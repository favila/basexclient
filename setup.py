from os.path import join, abspath, dirname
from setuptools import setup

here = abspath(dirname(__file__))

txtfiles = {}
for name in ['README.rst', 'LICENSE.txt']:
    with open(join(here, name)) as fp:
        txtfiles[name] = fp.read()

setup(
    name='BaseXClient',
    version='0.1.0',
    description='Client for the BaseX XML database server.',
    long_description=txtfiles['README.rst'],
    author='Francis Avila',
    author_email='francisga@gmail.com',
    license=txtfiles['LICENSE.txt'],
    packages=['basexclient', 'basexclient.test'],
    keywords=['BaseX', 'XML database', 'Server Protocol'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Text Processing :: Markup :: XML',
    ],
    scripts=['bin/basexconsole.py'],
    install_requires=[],
    tests_require=[
        'nose>=1.3.0',
        'coverage>=3.6',
    ],
    test_suite='nose.collector',
)
