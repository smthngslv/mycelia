# Concept

Mycelia is a task based interservice (interagent) communication system. It solves problem of creating and executing
multi steps, interconnected and very dependency heavy workflows along with data management in these workflows and
interservice (interagent) communication.

# Installation

You can clone latest version fron git.

```shell
UV_GIT_LFS=1 uv add "https://github.com/smthngslv/mycelia.git[logfire,rabbitmq,postgres,postgres-migrations]"
```

After that you need to create account in [Logfire](https://logfire.pydantic.dev/login) and run

```shell
logfire auth
```

It will open browser window and you can authenticate there. Then on very first run with you will be asked to
select/create project.

# Example

Start `postgres` and `rabbitmq`:

```shell
docker compose -f ./docs/docker-compose.yaml up -d
```

Initialize `postgres`:

```shell
alembic -x url=postgresql+asyncpg://mycelia:mycelia@localhost/mycelia upgrade head
```

Run example (server):

```python
python ./docs/example.py server
```

Run example (client):

```python
python ./docs/example.py client
```

In server console you will see:

```text
Hello from `get_random_number#G`, number is 2!
Hello from `get_random_number#H`, number is 1!
Hello from `get_random_number#I`, number is 2!
Hello from `get_random_number#A`, number is 1!
Hello from `get_random_number#C`, number is 1!
Hello from `get_random_number#B`, number is 1!
Hello from `get_random_number#E`, number is 2!
Hello from `get_random_number#D`, number is 2!
Hello from `get_random_number#F`, number is 1!
Hello from `get_random_number#Meow`, number is 1!
Hello from `print_numbers`, numbers are (2, 1, 2)!
```
