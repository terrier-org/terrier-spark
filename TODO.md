# Terrier-Spark TODO List

1. FeaturedQueryTransformer does not support remote indices*.
2. Features returned by FeaturedQueryTransformer cannot be changed for remote indices*.
3. We cannot perform parameter training on a remote index*.
4. Demonstrate relevance feedback.
5. Improve example simple run notebook: viz. creation of dataframes.
6. Create a feature ablation notebook.
7. Absolute values of RankingEvaluator not correct due to QrelTransformer not provided labels for unretrieved relevant documents. This is particularly true for MAP.
8. Toree complains about "error: error while loading UDTRegistration, class file '/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354/lib/spark2/jars/spark-catalyst_2.11-2.2.0.cloudera1.jar(org/apache/spark/sql/types/UDTRegistration.class)' has location not matching its contents: contains class UDTRegistration" etc.

Issues noted * require changes to Terrier
