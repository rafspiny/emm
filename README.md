# Evaluator of Massive Misalignment
EMM is a software to evaluate the performance degradation in relationional databases.

## The idea
Afer the talk from [XXX]() at [FOSDEM24](), I was intrigued by the idea of having a tool to help a person to design a DB table with some optimization in mind.

The idea is fairly simple. Given a DDL, the tool can create multiple schemas with different permutation of the order of the table's columns.

At this stage, the tool can populate the table, across the permucation schemas, so that we can
* analyze the improvements on the file system
* run workloads and analyze the (likely) performance improvements

## Recognition
The name is a tribute to an old friend of mine. Emmanuel Granatiello. He passed away too early. He has been a good person. Caring, empathetic, curious, rooted in his community, an hacker. He has been greatly missed.
This is a tiny gesture to remember a wonderful person, a friend that walked this rock and left too early.

## Notes
The initial release ecompasses PSQL only.

## How EMM works
EMM is easy to run. The `docker-compose` file defines two services:
* db
* emm-cli

You can bring up the services with your usual command for docker compose. Here we assume that you have a recent version, so `docker compose` instead of `docker-compose`.

```
docker compose up
```

You can then interact with the cli by running
```
docker exec emm-cli python emm.py <command> <arguments>
```

You are free to use poetry and create a virtual environment to run the cli locally, or access the docker container trough your favourite tool.
I prefer [lazydocker](https://github.com/jesseduffield/lazydocker), for instance.

### db service
We define a service based on the standard psql image with two volumes. One for the data and another for the initialization of the container for the first time.
It also exposes a port to allow you to connect to it from any other interface (PGAdmin or so).

You can also check what is in the DB by `docker exec emm-db psql -U emmuser emm`
