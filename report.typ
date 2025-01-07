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

== First solution: four jobs
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

Execution time: 3m 40s
```
2141	"Silence of the Lambs, The (1991)"
2169	Schindler's List (1993)
2361	"Godfather, The (1972)"
2534	Pulp Fiction (1994)
4036	"Shawshank Redemption, The (1994)"
```
