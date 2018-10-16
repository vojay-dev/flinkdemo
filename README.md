# Introduction

This repository contains various Flink examples that I created to prepare my talk at the code.talks conference 2018.

# Start Cluster

```
docker-compose up
```

# Volume Example

The volume examples are only working locally. You can start the App classes directly in your IDE.

# Twitter Example

The Twitter examples works locally as well as in the Docker setup.

Before running it either way, you need to create a `config.properties` file:

```
cp src/main/resources.properties{.dist,}
```
and enter your credentials.
