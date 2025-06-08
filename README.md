# Evaluator of Massive Misalignment
EMM is a software to evaluate the performance degradation in relational databases.

## The idea
After the talk from [Charly Batista](https://fosdem.org/2024/schedule/event/fosdem-2024-3601-reducing-costs-and-improving-performance-with-data-modeling-in-postgres/) at [FOSDEM24](https://fosdem.org/2024/), I was intrigued by the idea of having a tool to help a person designing a DB table with some optimizations in mind.

The idea is fairly simple. Given a table DDL, the tool creates a schema for the table under scrutiny and then creates multiple tables with different permutations of the table's columns.

At this stage, the tool can populate the tables, across the permutation schemas, so that we can
* analyze the improvements on the file system
* run workloads and analyze the (likely) performance improvements

## Recognition
The name is a tribute to an old friend of mine. Emmanuel Granatiello. He passed away too early. He has been an extraordinary person. Caring, empathetic, curious, rooted in his community, an hacker. He has been and is greatly missed.
[Charly Batista](https://fosdem.org/2024/schedule/event/fosdem-2024-3601-reducing-costs-and-improving-performance-with-data-modeling-in-postgres/) for giving the talk.

## Notes
The initial release encompasses PSQL only and it is written as I would be vibe coding. Whatever that means.

## How EMM works
The `docker-compose` file defines two services:
* `emm-db`
* `emm-cli`

You can bring up the services with your usual command for docker compose. Here we assume that you have a recent version, so `docker compose` instead of `docker-compose`.

```
docker compose up
```

### DB service
We define a service based on the standard psql image with two volumes. One for the data and another for the initialization of the container for the first time.
It also exposes a port to allow you to connect to it from any other interface (PGAdmin or so).

You can also check what is in the DB by `docker exec -it emm-db psql -U emmuser emm`

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
* The entry from the emm_project

Once you do that, there is no way back.
There is a `dry-run` option

```
$ docker exec emm-cli poetry run python __main__.py clean
Going to remove schema raf_emm. Dry run is True
No schema was removed. dry-run is True
✔ /data/projects/emm [main|✚ 3]

```

##### permutations
Generates permutations of the columns. It takes two parameters in input:
* schema-name: the name of the schema you want to analyze
* permutation-type
  * all
  * type

```
$ docker exec emm-cli poetry run python __main__.py permutations --schema-name raf_emm --permutation-logic type
Permutations generated
✔ /data/projects/emm [main|✚ 3]
```

##### populate
Generates data to use to populate all the permutations tables that have been created with teh permutations command.
It takes two parameters:
* schema-name: Schema name to populate
* only-original: If one wants to populate only the original table, not the permutations.

```
$ docker exec emm-cli poetry run python __main__.py populate --schema-name raf_emm
DEBUG:src.emm.engine.parser:Loading DDL for project raf_emm at location sql/projects/raf_emm/data.sql
Schema populated
✔ /data/projects/emm [main|✚ 3]
```

##### benchmark
It runs the benchmark on the original table and on all the permutations.
It takes two parameters:
* schema-name: Schema name to benchmark
* benchmark-logic: 
  * all
  * size
  * ro

```
$ docker exec emm-cli poetry run python __main__.py benchmark --schema-name raf_emm --benchmark-logic all
INFO:src.cli:Value type not valid. Defaults to all.
Schema raf_emm benchmark finished.
✔ /data/projects/emm [main|✚ 3]
```

##### report
It generates a report of the benchmark.
It takes one parameter:
* schema-name: Schema name to report

```
$ docker exec emm-cli poetry run python __main__.py report --schema-name raf_emm
Loading schema analysis
Analysis raf_emm_disk_analysis
| Metric      | Best Permutation   | Improvement (%)   |   Size Table |
|-------------|--------------------|-------------------|--------------|
| total_bytes | raf_emm_0231       | 0.74%             |      1000000 |
| index_bytes | raf_emm_2310       | 0.00%             |      1000000 |
| toast_bytes | raf_emm_2310       | 0.00%             |      1000000 |
| table_bytes | raf_emm_0231       | 0.91%             |      1000000 |

✔ /data/projects/emm [main|✚ 3]
```


## TODO
* [ ] Add flask tests for RO, RW and mix workload.
* [ ] Add generator for data.sql
* [ ] Maybe using pgbench instead
* [ ] Consider rewriting it with go or rust