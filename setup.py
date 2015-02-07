"""
Setup file for VSAT
"""

import os
from setuptools import setup

def read(fname):
    """
    Utility that reads a file and returns it.
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="vsat",

    version=read("VERSION"),

    author="Robin Ridler",

    author_email="robin.ridler@gmail.com",

    description=("Very simple asynchrous tasks for Python."),

    license="BSD",

    keywords="Simple Asynchronous Tasks",

    url="http://",

    packages=['vsat'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],

    install_requires=[

    ]
)
