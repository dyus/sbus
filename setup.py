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
    packages=find_packages(exclude=('*.tests', 'tests.*')),
    include_package_data=True,
    license='BSD License',
    description='Pub/sub absctraction for aioamqp',
    install_requires=[
        'aioamqp==0.10.0',
        'pydantic==0.6.3'
    ],
    extras_require={
        'test': [
            'pytest==3.5.1',
            'pytest-asyncio==0.6.0',
            'pytest-flake8==1.0.1',
            'pytest-isort==0.1.0',
        ]
    },
    url='https://github.com/dyuuus/sbus'
)
