# Hadoop project

Project for the Large Scale Distributed System's course at the Université Côte d'Azur (ubinet track).

## Complile

`mvn package`

## Useful commands

```bash
time hadoop jar hadoop-1.0.jar app.ChainSec /input /output
hdfs dfs -cat /user/root/intermediate_output1/part-r-00000
hdfs dfs -cat /output/part-r-00000
```

