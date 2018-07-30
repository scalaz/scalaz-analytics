# scalaz-analytics

[![Gitter](https://badges.gitter.im/scalaz/scalaz-analytics.svg)](https://gitter.im/scalaz/scalaz-analytics?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Goal

Scalaz Analytics provides a high-performance, purely-functional library for doing computational analysis and statistics over data in a type-safe way.


## Introduction & Highlights

Scalaz Analytics is a principled functional programming library for data processing and analytics. 

- Simple and principled
- First class support for analytics and data science
- Pure type-safe, functional interface that integrates with other Scalaz projects
- Supports batch and streaming
- Efficient on both small and large data sets, single machine and distributed
- Can be used from a REPL for interactive analysis or as a library for applications


## Other libraries

Below is a selection of Analytics/Data processing Libraries that we are being used as inspiration. 
Some of these metrics are somewhat subjective but they give an idea for what we are looking at from each library. 
Note that these metrics assume native support, so libraries that achieve these things via another library are not considered.

| Library | Scales to Big Data | Supports Batch | Supports Streaming | FP | Easy to Debug | Out of the box analytics |
| --- | --- | --- | --- | --- | --- | --- |
| [Spark](https://spark.apache.org/) | ✔ | ✔ | ✔ (mini batch) | ✘ | ✘ | ✘ | 
| [Flink](https://flink.apache.org/) | ✔ | ✔ | ✔ | ✘ | ✘ | ✘ |
| [Pandas](https://pandas.pydata.org/) | ✘ | ✔ | ✘ | ✘ | ✔ | ✘ |
| [R](https://www.r-project.org/) | ✘ | ✔ | ✔ | ✘ | **?** | ✔ |  
| [Dask](https://dask.pydata.org/) | ✔ | ✔ | ✔ | ✘ | **?** | ✘ |
| [Apex](https://apex.apache.org/) | ✔ | ✔ | ✔ | ✘ | **?** | ✘ |
| [Beam](https://beam.apache.org/) | ✔ | ✔ | ✔ | ✘ | **?** | ✘ |

## Background

* [Apache Spark](https://spark.apache.org/) - [Internal Data Formats](https://spoddutur.github.io/spark-notes/deep_dive_into_storage_formats), [Tungsten 1](https://spoddutur.github.io/spark-notes/second_generation_tungsten_engine.html)/[Tungsten 2](https://developer.ibm.com/code/2016/12/16/bringing-apache-spark-closer-simd-gpu/) and [Catalyst](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html) optimisations, [Mastering Apache Spark](https://legacy.gitbook.com/book/jaceklaskowski/mastering-apache-spark/details), [Frameless - a library that adds type-safety to Spark](https://github.com/typelevel/frameless)
* [Quark](https://github.com/slamdata/quark) - Functional Data Processing DSL - [presentation](https://www.slideshare.net/jdegoes/quark-a-purelyfunctional-scala-dsl-for-data-processing-analytics) and [video](https://www.youtube.com/watch?v=_-GD8VJW8jU)
* [Flink](https://ci.apache.org/projects/flink/flink-docs-master/) documentation
* [Apache Arrow](https://arrow.apache.org/) - High performance in memory data platform
* [Icicle](https://github.com/ambiata/icicle) - Optimisation of DSLs into efficient code for data processing [talk](https://www.youtube.com/watch?v=ZuCRgghVR1Q)
* [Machine Fusion](https://www.cse.unsw.edu.au/~amosr/papers/robinson2017merges.pdf)
* [An algebra for distributed big data analytics - Leonidas Fegaras](https://lambda.uta.edu/mrql-algebra.pdf) 
* [Compiling to categories -  Conal Elliott](http://conal.net/papers/compiling-to-categories/)
* [Linear algebra for data processing](http://www4.di.uminho.pt/~jno/html/jnopub.html) and other [similar work](https://link.springer.com/article/10.1007%2Fs00165-014-0316-9#page-1) by José Nuno Oliveira
* [Data cube as typed linear algebra operator](http://www4.di.uminho.pt/~jno/ps/dbpl17sl.pdf)
* Comprehensions syntax for query languages [Monoid](http://www.acidalie.com/monoid-comprehension.html) and [others](https://db.inf.uni-tuebingen.de/publications/TakeEverythingFromMe-ButLeaveMetheComprehension.html)
* Efficient derivation of folds in haskells [foldl](https://hackage.haskell.org/package/foldl)
* Mining Massive Data Sets [free online book](http://www.mmds.org/)
* Scipy [stats package](https://docs.scipy.org/doc/scipy/reference/tutorial/stats.html)
* Concurrent commutative folds in [Tesser](https://github.com/aphyr/tesser)
* Explanation of [zero copy](https://www.ibm.com/developerworks/library/j-zerocopy/index.html)
