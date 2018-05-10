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
