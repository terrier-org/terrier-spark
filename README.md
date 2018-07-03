# Terrier-Spark

Terrier-Spark is a Scala library for [Apache Spark](https://spark.apache.org/) that allows the [Terrier.org](http://terrier.org) information retrieval platform to be installed and working.

To use within a notebook, this requires [Apache Toree](https://toree.apache.org/) to be installed and working.

Requirements:
 - Terrier 5.0
 - Apache Spark version 2.0 or newer
 - Jupyter & Apache Tree (optional)

## Functionality

 - Retrieving a run from a Terrier index (local or remote)
 - Evaluating a run
 - Optimising the parameter of a retrieval run on a local index
 - Grid searching the parameter of a retrieval run on a local index
 - Learning a model using learning-to-rank

For known improvements/issues, see [TODO.md](TODO.md)

## Example

    val indexref = IndexRef.of("/path/to/index/data.properties")

    val props = Map(
    "terrier.home" -> terrierHome)

    TopicSource.configureTerrier(props)
    val topics = TopicSource.extractTRECTopics(topicsFile)
        .toList.toDF("qid", "query")

    val queryTransform = new QueryingTransformer()
        .setTerrierProperties(props)
        .setIndexReference(indexref)
        .setSampleModel(model)

    val r1 = queryTransform.transform(topics)
    //r1 is a dataframe with results for queries in topics
    val qrelTransform = new QrelTransformer()
        .setQrelsFile(qrelsFile)

    val r2 = qrelTransform.transform(r1)
    //r2 is a dataframe as r1, but also includes a label column
    val ndcg = new RankingEvaluator(Measure.NDCG, 20).evaluateByQuery(r2).toList


## Use from the Spark Shell

	$ spark-shell --packages org.terrier:terrier-spark:0.0.1


## Use within a Jupyter Notebook

Firstly, make sure you have a working installation of Toree.

We have provided several example notebooks:
 - Performing a simple run: [example_notebooks/toree/simple_run.ipynb](example_notebooks/toree/simple_run.ipynb)
 - Training a weighting model parameters: [example_notebooks/toree/train_bm25.ipynb](example_notebooks/toree/train_bm25.ipynb)
 - Training and evaluating a learning-to-rank model: [example_notebooks/toree/ltr.ipynb](example_notebooks/toree/train_bm25.ipynb)

## Bibliography

If you use this software, please cite one of:

1. Combining Terrier with Apache Spark to create agile experimental information retrieval pipelines. Craig Macdonald. In Proceedings of SIGIR 2018.

2. Agile Information Retrieval Experimentation with Terrier Notebooks. Craig Macdonald, Richard McCreadie, Iadh Ounis. In Proceedings of DESIRES 2018.

## Credits

Developed by Craig Macdonald, University of Glasgow
