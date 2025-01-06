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

= 