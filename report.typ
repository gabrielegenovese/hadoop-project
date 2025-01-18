#import "@preview/rubber-article:0.1.0": *

#show link: underline
#show: article.with()

#set text(
  lang: "en"
)

#maketitle(
  title: "Report for Large Scale Distributed Systems' Project",
  authors: (
    "Gabriele Genovese",
  ),
  date: datetime.today().display("[day] [month repr:long] [year]"),
)

= Hadoop Project
This project was about creating complex queries on a distributed filesystem. Using the `hadoop` library for `Java`, the main solution was to create a series of Map/Reduce jobs in order to extract how many times a movie was the favourite among the users. I'll present multiple solution to find out the best approch to the problem.

In order to set up the environment to run the project create the `data` folder, move the unzipped data inside it and use the followings commands:
```shell
docker compose up -d
docker exec -it namenode /bin/bash
hdfs dfs -mkdir /input
hdfs dfs -put /hadoop/labs/movie-data/movies.csv /input
hdfs dfs -put /hadoop/labs/movie-data/ratings.csv /input
```

*N.B.:* after running the project, if you want to run it again, there is no need to delete the `output` or the `intermediate_output` folders because they are automatically removed by the program.

== First solution: four jobs

The first solution combine 4 jobs to reach the desired result. It fully use the Map/rerduce pattern philosofy.

=== Design of the solution
This solution is divided in four jobs:
- the first one is in charge of joining the two file using movieId as a key
- the second one format the data to use userId as a key
- the third one choose a random favorite movie per user
- the fourth compute the frequency and format the output

In the code, there are comments over each function that specify how the input and the output of each map reduce is managed.

=== Execution

To execute the code, open a new terminal and use the followings commands in the host machine:

```shell
mvn package
cp target/hadoop-1.0.jar data
```

Then, run this command in the `namenode` container: 

`time hadoop jar /hadoop/labs/hadoop-1.0.jar app.ChainFirst /input /output`

The program should give the following output:

#figure(
  image("assets/first.png"),
  caption: [First's solution output showed using `hdfs dfs -cat /output/part-r-00000`]
)

The solution run in about 4 minutes and 13 seconds. The most favoured movie was "The Shawshank Redemption" (1994) with 4036 preference. 

== Other solutions: three and two jobs

It's also possible to execute different kind of solutions with lower jobs. This was done because I noted that creating an entire new job is an heavy operation, so I wanted to explore how the execution time can improve using less jobs. 

The two other solutions can be executed using:

```shell
time hadoop jar /hadoop/labs/hadoop-1.0.jar app.ChainSec /input /output
time hadoop jar /hadoop/labs/hadoop-1.0.jar app.ChainTer /input /output
```

The second solution with three jobs ends in about 3 minutes and 40 minutes. The third solution with two jobs ends in about 2 minutes and 8 minutes.

#figure(
  image("assets/second.png", width: 68%),
  caption: [Second's solution output]
)


#figure(
  image("assets/third.png", width: 68%),
  caption: [Third's solution output (note that is different from the other two solutions, this is due to the randomness of chosing a user's favorit movie)],
)

