#!/usr/bin/env python
"""setup.py

.. codeauthor:: John Lane <john.lane93@gmail.com>

"""

import os
from setuptools import setup
from eve_dynamodb import __author__, __email__, __license__, __version__


def read(filename):
    """Utility function to read the README file

    :param str filename: File name
    :return:
    """

    return open(os.path.join(os.path.dirname(__file__), filename)).read()


setup(
    name="eve_dynamodb",
    version=__version__,
    author=__author__,
    author_email=__email__,
    maintainer=__author__,
    maintainer_email=__email__,
    description="DynamoDB data layer access for Eve REST API",
    license=__license__,
    keywords="flask eve dynamodb rest api",
    packages=["eve_dynamodb"],
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    install_requires=[
        "awscli>=1.16.255"
        "boto3>=1.9.245",
        "Eve>=0.9.0",
        "Flask>=1.1.0",
    ],
    python_requires=">=3.6",
    setup_requires=["pytest-runner"],
    tests_require=[
        "pytest>=5.2.0",
        "pytest-cov>=2.6.0",
        "pytest-flask>=0.15.0",
        "pytest-pep8>=1.0.0",
        "pytest-pylint>=0.14.0"
    ],
    extras_require={
        "release": [
            "bumpversion>=0.5.0",
            "Sphinx>=2.0.0",
            "sphinx-rtd-theme>=0.4.0"
        ]
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Flask",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython"
    ]
)
