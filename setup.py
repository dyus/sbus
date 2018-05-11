#!/usr/bin/env python
import os

from setuptools import find_packages, setup

from service_bus import __version__

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='sbus',
    version=__version__,
    author='Andrey Zdanchuk',
    author_email='dyuuus@gmail.com',
    packages=find_packages(),
    include_package_data=True,
    license='BSD License',
    description='Pub/sub absctraction for aioamqp',
    install_requires=[
        'aioamqp==0.10.0',
        'pydantic==0.6.3'
    ],
    tests_require=[
        'pytest-asyncio',
        'pytest-flake8',
        'pytest-isort',
        'pytest'
    ],
    setup_requires=['pytest-runner'],
    url='https://github.com/dyuuus/sbus'
)
