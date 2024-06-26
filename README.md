# Evaluator of Massive Misalignment
EMM is a software to evaluate the performance degradation in relational databases.

## The idea
After the talk from [Charly Batista](https://fosdem.org/2024/schedule/event/fosdem-2024-3601-reducing-costs-and-improving-performance-with-data-modeling-in-postgres/) at [FOSDEM24](https://fosdem.org/2024/), I was intrigued by the idea of having a tool to help a person to design a DB table with some optimization in mind.

The idea is fairly simple. Given a DDL, the tool can create multiple schemas with different permutation of the order of the table's columns.

At this stage, the tool can populate the table, across the permutation schemas, so that we can
* analyze the improvements on the file system
* run workloads and analyze the (likely) performance improvements

## Recognition
The name is a tribute to an old friend of mine. Emmanuel Granatiello. He passed away too early. He has been an extraordinary person. Caring, empathetic, curious, rooted in his community, an hacker. He has been and is greatly missed.

## Notes
The initial release encompasses PSQL only.

## How EMM works
The `docker-compose` file defines two services:
* db
* emm-cli

You can bring up the services with your usual command for docker compose. Here we assume that you have a recent version, so `docker compose` instead of `docker-compose`.

```
docker compose up
```

### DB service
We define a service based on the standard psql image with two volumes. One for the data and another for the initialization of the container for the first time.
It also exposes a port to allow you to connect to it from any other interface (PGAdmin or so).

You can also check what is in the DB by `docker exec emm-db psql -U emmuser emm`

### CLI servie

You can then interact with the cli by running
```
docker exec emm-cli poetry run python __main__.py <command> <arguments>
```

You are free to use poetry and create a virtual environment to run the cli locally, or access the docker container trough your favourite tool.
I prefer [lazydocker](https://github.com/jesseduffield/lazydocker), for instance.

To get the list of available commands use `docker exec emm-cli poetry run python __main__.py`
```
Usage: __main__.py [OPTIONS] COMMAND [ARGS]...

  EMM is designed to help you find a byte-alignment-optimized columns order

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  benchmark  Run benchmarks
  clean      Remove all the schemas from the DB.
  env        Print env variables
  init       Init the schema based on the file sql/init/*.sql
  ls         List all the schemas present in the DB.
  perms      Generates permutations based on the table defined in your...
  populate   Insert data from file sql/data/*.sql into the table
```

#### Commands
##### init
It initializes a database and keep track of tit in the `emm` schema.
It takes one parameter in input that is the directory name where the files for the DB schema and data are stored.

The expected file system structure is this:

```
.
├── ...
├── README.md
├── sql
│   ├── project_emm
│   │   └── schemas.sql
│   ├── projects
│   │   └── raf_emm
│   │       ├── data.sql
│   │       └── schema.sql
│   └── raf_emm
└── src
    ├── cli.py
    ├── emm
    │   ├── ...

```
Under the `sql/projects` directory, you can place as many schemas you want to analyze as long as you create a directory that contains two files:
* schema.sql
* data.sql

**schema.sql** contains the definition of the table you want to analyze.
**data.sql** contains the insert for the table contained in schema.sql

The name of the directory is the name of your project.

```
$ docker exec emm-cli poetry run python __main__.py init --sql-folder-path raf_emm
DEBUG:src.emm.operations.validators:Checking raf_emm folder existence in /home/emm
DEBUG:src.emm.operations.validators:Found sql/projects/raf_emm folder
Schema initialized
```
##### ls
It will list all the available projects
```
$ docker exec emm-cli poetry run python __main__.py ls
Found schema: raf_emm
✔ /data/projects/emm [main|✚ 3]
21:47 $
```

##### clean
Remove all projects from the DB. It removes:
* The tables
* All the permutations
* The entry from the emm_schema

Once you do that, there is no way back.
There is a `dry-run` option

```
$ docker exec emm-cli poetry run python __main__.py clean
Going to remove schema raf_emm. Dry run is True
No schema was removed. dry-run is True
✔ /data/projects/emm [main|✚ 3]

```
