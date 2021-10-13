# intraday-project
Repository for "Cryptocurrencies and Stablecoins: a high frequency analysis"

E. Barucci, G. Giuffra, D. Marazzina

## Requirements
- https://git-lfs.github.com/: since some of the files in the data directory are larger than the 100 MB natively handled by github.
- https://www.scala-sbt.org/: to compile the Scala project.
- https://spark.apache.org/: given the amount of data it was necessary to use this framework for big data analytics.
- In the `python` directory, the file `requirements.txt` contains the dependencies to run the different python scripts.

## Structure
- `data`: it contains all data and is divided in the following subdirectories:
  -  `provider-data`: it contains a small sample of the proprietary data leased from the data provider. We cannot include the whole sample since it will be a breach of contract. In addition, the total amount of data is around 5 TB.
  - `processed-data`: Scala and some python scripts write the output of the spark jobs in this directory. Scripts also read from this directory as well since the processing of the data is composed of various steps.
  - `clean-time-series`: Scala scripts write to this directory the cleaned time series that is then used by some python scripts for time series analysis.
- `emr`: it contains the script to launch the Spark scripts in Amazon EMR (https://aws.amazon.com/emr/), as well as configuration files for the cluster and the jobs to be performed by the cluster.
- `python`: it contains all python scripts. Some are Spark scripts and others are scripts to generate tables, figures, and perform time series analysis.
- `scripts`: it contains the bash scripts to run the Spark jobs.
- `src`: it contains all Scala classes. It follows the structrue required by the `sbt` tool.

## Output
- Tables 2-7 are obtained with Scala (`src` folder)
- Tables 8-13 are obtained with Python (`python` folder)
Due to the fact that the folder `provider-data` contains a small sample of the proprietary data leased from the data provider, it is not possible to obtain the same values of the tables in the article.
