[![PyPI version](https://badge.fury.io/py/sbus.svg)](https://badge.fury.io/py/sbus) [![Build Status](https://travis-ci.org/dyus/sbus.svg?branch=master)](https://travis-ci.org/dyus/sbus) [![Coverage Status](https://coveralls.io/repos/github/dyus/sbus/badge.svg?branch=master)](https://coveralls.io/github/dyus/sbus?branch=master)

# Testing

There are two ways to execute tests (docker required):

```bash
# all tests will be executed in docker container
docker-compose -f docker/docker-compose.yml run tests

# or start RabbitMQ only and execute tests locally
docker-compose -f docker/docker-compose.yml up rabbitmq
export RABBITMQ_HOST=<docker host>
pip install -e .[test]
pytest
```
