## cassandra-migrator

Spark jobs in this repo can be used for data migration from one Cassandra cluster to another.

> This repo has been cloned and re-factored from [cassandra-data-migrator](https://github.com/datastax/cassandra-data-migrator)

## Prerequisite

1. Docker container with Spark, example: [pyspark-notebook](https://github.com/jupyter/docker-stacks/tree/main/pyspark-notebook)
2. [IntelliJ IDEA](https://www.jetbrains.com/idea/download/)

# Steps:

1. `sparkConf.properties` file needs to be configured as applicable for the environment
   > A sample Spark conf file configuration can be [found here](./src/resources/sparkConf.properties)
2. Place the conf file where it can be accessed while running the job via spark-submit.
3. Generate a jar (`cassandra-migrator.jar`) using IntelliJ
4. Run the 'Data Migration' job using `spark-submit` command as shown below:

```
./spark-submit --properties-file sparkConf.properties /
--master "local[*]" /
--class com.bd.scala.Migrate cassandra-migrator.jar &> logfile_name.txt
```

Note: Above command also generates a log file `logfile_name.txt` to avoid log output on the console.