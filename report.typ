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
This project has multiple solution to find out the best approch to the problem


== First solution: four jobs
=== Design of the solution
This solution is divided in four jobs:
- the first one is in charge of joining the two file using movieId as a key
- the second one format the data to use userId as a key
- the third one choose a random favorite movie per user
- the fourth compute the frequency and format the output

=== Execution
Exec command: `time hadoop jar hadoop-1.0.jar app.ChainFirst /input /output`

#image("assets/first.png")

Execution time: 4m 13s

Last 5 lines of output: `hdfs dfs -cat /output/part-r-00000`
```
...
2141	"Silence of the Lambs, The (1991)"
2169	Schindler's List (1993)
2361	"Godfather, The (1972)"
2534	Pulp Fiction (1994)
4036	"Shawshank Redemption, The (1994)"
```

== Second solution: three jobs
Exec command: `time hadoop jar hadoop-1.0.jar app.ChainSec /input /output`

#image("assets/second.png")

Execution time: 3m 40s
```
2141	"Silence of the Lambs, The (1991)"
2169	Schindler's List (1993)
2361	"Godfather, The (1972)"
2534	Pulp Fiction (1994)
4036	"Shawshank Redemption, The (1994)"
```
== Third solution: two jobs
Exec command: `time hadoop jar hadoop-1.0.jar app.ChainTer /input /output`

#image("assets/third.png")

Execution time: 2m 8s
```
3719	Schindler's List (1993)
4692	Inglourious Basterds (2009)
5243	"Lord of the Rings: The Two Towers, The (2002)"
9066	"Silence of the Lambs, The (1991)"
9487	Fargo (1996)
```

