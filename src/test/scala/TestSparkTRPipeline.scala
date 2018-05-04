

import scala.reflect.runtime.universe
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import org.terrier.spark.eval.RankingMetrics2
import org.terrier.spark.ml.FeaturesQueryingTransformer
import org.terrier.spark.ml.QrelTransformer
import org.terrier.spark.ml.QueryingTransformer
import org.terrier.spark.LTRPipeline
import org.terrier.spark.ml.NDCGEvaluator
import org.terrier.spark.ml.RankingEvaluator
import org.terrier.spark.ml.ReplaceScoreTransformer
import org.terrier.spark.ml.ArbitraryParameterTrainingEstimator
import org.terrier.spark.ml.ReplaceScoreTransformer


class TestSparkTRPipeline extends FlatSpec {
  
  
  def getSpark() = {
    SparkSession
    .builder()
    .config("spark.master", "local")
    .getOrCreate()
//    val sConf = new SparkConf()
//    sConf.setMaster("local")
//    sConf.setAppName("tester")
//    val sc = new SparkContext(sConf)
//    sc
  }
  
  "WT2G" should "work for grid tuning BM25 retrieval" in {
    import spark.implicits._
    
    val props = Map(
            "terrier.home" -> "/Users/craigm/git/Terrier",
            "terrier.etc" -> "/Users/craigm/git/Terrier/etc",
            "terrier.index.path" -> "/Users/craigm/wt2g_index/index/")
    
    LTRPipeline.configureTerrier(props)
    
    val allTopicsList = LTRPipeline.extractTRECTopics("/Users/craigm/wt2g_index/topicsqrels/small_web/topics.401-450").toList
    val Array(trTopics, teTopics) = allTopicsList.toDF("qid", "query").randomSplit(Array(0.5,0.5), 130882)
    val queryTransform = new QueryingTransformer()
      .setTerrierProperties(props)
      .setSampleModel("BM25")
      
    val propertyName = "c"
    
    val qrelTransform = new QrelTransformer()
        .setQrelsFile("/Users/craigm/wt2g_index/topicsqrels/small_web/qrels.trec8")
        
    val pipeline = new Pipeline().setStages(
        Array(queryTransform, qrelTransform))
    
     val paramGrid = new ParamGridBuilder()
      .addGrid(queryTransform.localTerrierProperties, Array(
          props + (propertyName -> "0.1"), 
          props + (propertyName -> "0.2"), 
          props + (propertyName -> "0.3"), 
          props + (propertyName -> "0.4"), 
          props + (propertyName -> "0.5"), 
          props + (propertyName -> "0.6"), 
          props + (propertyName -> "0.7"), 
          props + (propertyName -> "0.8"), 
          props + (propertyName -> "0.9")))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new NDCGEvaluator(20))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)
    val model = cv.fit(trTopics)
    
    val rtrUntrained = qrelTransform.transform(queryTransform.transform(teTopics))
    val rtrTrained = model.transform(teTopics)
    
    val metricsUntrained = getRankingMetrics(rtrUntrained)
    val metricsTrained = getRankingMetrics(rtrTrained)
    
    System.out.println("*** UNTRAINED MAP@1000 " + metricsUntrained.meanAveragePrecision(1000))
    System.out.println("*** UNTRAINED NDCG@20 " + metricsUntrained.ndcgAt(20))
    
    System.out.println("*** RESULTING MAP@1000 " + metricsTrained.meanAveragePrecision(1000))
    System.out.println("*** RESULTING NDCG@20 " + metricsTrained.ndcgAt(20))
    System.out.println("*** RESULTING CV_MODEL params " + model.bestModel.extractParamMap() )
  }
  
  "WT2G" should "work for numerical optimising C for BM25" in {
    import spark.implicits._
    val props = Map(
            "terrier.home" -> "/Users/craigm/git/Terrier",
            "terrier.etc" -> "/Users/craigm/git/Terrier/etc",
            "terrier.index.path" -> "/Users/craigm/wt2g_index/index/")
    
    LTRPipeline.configureTerrier(props)
    
    val allTopicsList = LTRPipeline.extractTRECTopics("/Users/craigm/wt2g_index/topicsqrels/small_web/topics.401-450").toList
    val Array(trTopics, teTopics) = allTopicsList.toDF("qid", "query").randomSplit(Array(0.5,0.5), 130882)
    
    val tuner = new ArbitraryParameterTrainingEstimator()
    tuner.setTerrierProperties(props)
    tuner.set(tuner.paramName, Seq("c"))
    tuner.setSampleModel("BM25")
    tuner.set(tuner.paramValueInitial, Seq(0.25d))
    tuner.set(tuner.paramValueMin, 0d)
    tuner.set(tuner.paramValueMax, 1d)
    tuner.set(tuner.measureTol, 1e-4)
    tuner.set(tuner.optMaxIter, 100)
    
    tuner.setQrelsFile("/Users/craigm/wt2g_index/topicsqrels/small_web/qrels.trec8")
    val model = tuner.fit(trTopics)
    println(model.propertySettings.get("c").get)
  }
  
  "WT2G" should "work for numerical optimising BM25F" in {
    import spark.implicits._
    val props = Map(
            "terrier.home" -> "/Users/craigm/git/Terrier",
            "terrier.etc" -> "/Users/craigm/git/Terrier/etc",
            "terrier.index.path" -> "/Users/craigm/wt2g_index/index/")
    
    LTRPipeline.configureTerrier(props)
    
    val allTopicsList = LTRPipeline.extractTRECTopics("/Users/craigm/wt2g_index/topicsqrels/small_web/topics.401-450").toList
    val Array(trTopics, teTopics) = allTopicsList.toDF("qid", "query").randomSplit(Array(0.5,0.5), 130882)
    
    val fields = Seq(0,1,2)
    val cParams = fields.map{f =>  
      {
        //what parameter are we optimising?
        val param = "c."+f
        //set the weight of this field to 1, all other fields to 0
        val fieldsWeightParams = 
           Map("w."+f -> "1") ++
          fields.filter { fi => fi != f }.map(fi => ("w."+fi -> "0"))
        
        val tuner = new ArbitraryParameterTrainingEstimator()
        tuner.setTerrierProperties(props ++ fieldsWeightParams)
        tuner.set(tuner.paramName, Seq(param))
        tuner.setSampleModel("BM25F")
        tuner.set(tuner.paramValueInitial, Seq(0.25d))
        tuner.set(tuner.paramValueMax, 1d)
        tuner.set(tuner.paramValueMin, 0d)
        tuner.set(tuner.measureTol, 1e-4)
        tuner.set(tuner.optMaxIter, 100)
        tuner.setQrelsFile("/Users/craigm/wt2g_index/topicsqrels/small_web/qrels.trec8")
        val model = tuner.fit(trTopics)
        (f,model.propertySettings.get(param).get)
      }
    }
    
    println(cParams)
    
  }
  
   "WT2G" should "work for LTR retrieval" in {
    
    import spark.implicits._
    
    val props = Map(
            "proximity.dependency.type" -> "SD",
            "terrier.home" -> "/Users/craigm/git/Terrier",
            "terrier.etc" -> "/Users/craigm/git/Terrier/etc",
            "terrier.index.path" -> "/Users/craigm/wt2g_index/index/")
    
    LTRPipeline.configureTerrier(props)
    
    val allTopicsList = LTRPipeline.extractTRECTopics("/Users/craigm/wt2g_index/topicsqrels/small_web/topics.401-450").toList
    val Array(trTopics, teTopics) = allTopicsList.toDF("qid", "query").randomSplit(Array(0.5,0.5), 130882)
    
    System.out.println("found " + allTopicsList.size + " topics in allTopics df")
        
    val queryTransform = new FeaturesQueryingTransformer()
      .setTerrierProperties(props)
      .setRetrievalFeatures(List( 
          "WMODEL:BM25", 
          "WMODEL:PL2",
          "DSM:org.terrier.matching.dsms.DFRDependenceScoreModifier"))
      .setSampleModel("InL2")
    
    val qrelTransform = new QrelTransformer()
        .setQrelsFile("/Users/craigm/wt2g_index/topicsqrels/small_web/qrels.trec8")
    
    val useRF: Boolean = false
        
    val learner = 
      if(useRF) new RandomForestRegressor()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setPredictionCol("newscore")
      else new LinearRegression()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(10)
        .setRegParam(0.01)
        .setElasticNetParam(0.8)
        .setPredictionCol("newscore")
      
    val replaceScore = new ReplaceScoreTransformer()
        
    val pipeline = new Pipeline().setStages(
        Array(queryTransform, qrelTransform, learner, replaceScore))
        
     val paramGrid = new ParamGridBuilder()
      .addGrid(queryTransform.sampleModel, Array("InL2", "BM25", "PL2"))
      .addGrid(learner.asInstanceOf[LinearRegression].regParam,  Array(0.1, 0.01))
      .build()
      
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new NDCGEvaluator(20))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)
    val model = cv.fit(trTopics)   
       
    val rtrUntrained = qrelTransform.transform(queryTransform.transform(teTopics))
    val rtrTrained = model.transform(teTopics)
    
    val metricsUntrained = getRankingMetrics(rtrUntrained)
    val metricsTrained = getRankingMetrics(rtrTrained)
    
    System.out.println("*** UNTRAINED MAP@1000 " + metricsUntrained.meanAveragePrecision(1000))
    System.out.println("*** UNTRAINED NDCG@20 " + metricsUntrained.meanNDCGAt(20))
    
    System.out.println("*** RESULTING MAP@1000 " + metricsTrained.meanAveragePrecision(1000))
    System.out.println("*** RESULTING NDCG@20 " + metricsTrained.meanNDCGAt(20))
    System.out.println("*** RESULTING CV_MODEL params " + model.bestModel.extractParamMap() )
  }
   
  def getRankingMetrics(df : Dataset[_]) : RankingMetrics2[Int] = {
    import df.sqlContext.implicits._
    import org.apache.spark.sql.functions._
    val xdd = df.orderBy(asc("qid"), desc("score")).
      select("qid", "rank", "score", "label").map{
        case Row(qid: String, rank: Int, score: Double, label : Int) =>
        (qid,rank,score,label)
      }.rdd
    val xee = xdd.groupBy(_._1)
    val xff = xee.map{case (qid : String, docs : Iterable[(String,Int,Double,Int)])  =>
      (docs.map{item => item._2}.toArray, docs.map{item => (item._2 -> item._4)}.toMap)          
    }
    val xgg = xff.toLocalIterator.toSeq
    new RankingMetrics2[Int](xgg)
  }
        
//  def dfZipWithIndex(
//    df: Dataset[_],
//    offset: Int = 1,
//    colName: String = "id",
//    inFront: Boolean = true
//  ) : Dataset[_] = {
//    df.sqlContext.createDataFrame(
//      df.rdd.zipWithIndex.map(ln =>
//        Row.fromSeq(
//          (if (inFront) Seq(ln._2 + offset) else Seq())
//            ++ ln._1.toSeq ++
//          (if (inFront) Seq() else Seq(ln._2 + offset))
//        )
//      ),
//      StructType(
//        (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]()) 
//          ++ df.schema.fields ++ 
//        (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
//      )
//    ) 
//  }
 
  "WT2G" should "work for retrieval" in {
    
    import spark.implicits._
    
    val props = Map(
            //"stopwords.filename" -> "resource:/stopword-list.txt",
            "terrier.home" -> "/Users/craigm/git/Terrier",
            "terrier.etc" -> "/Users/craigm/git/Terrier/etc",
            "terrier.index.path" -> "/Users/craigm/wt2g_index/index/")
    
    LTRPipeline.configureTerrier(props)
    
    val trainingTopicIter = LTRPipeline.extractTRECTopics("/Users/craigm/wt2g_index/topicsqrels/small_web/topics.401-450")
    val trainingTopics = trainingTopicIter.toList.toDF("qid", "query")
    
    System.out.println("found " + trainingTopics.count() + " topics in trainingTopics df")
        
    val queryTransform = new QueryingTransformer()
      .setTerrierProperties(props)
    
    val r1 = queryTransform.transform(trainingTopics)
      
    val qrelTransform = new QrelTransformer()
        .setQrelsFile("/Users/craigm/wt2g_index/topicsqrels/small_web/qrels.trec8")
    
    val r2 = qrelTransform.transform(r1)
    
    r2.printSchema()
    System.out.println(r2.first().toString())
//    val pipeline = new Pipeline().setStages(
//        Array(queryTransform, qrelTransform))
//    pipeline.fit(trainingTopics)
    
  }
  
  val spark = getSpark()
  
  "twoTopics" should "work" in {
    
    import spark.implicits._
    
    val trainingTopics = spark.createDataFrame(Seq(
        ("1", "this is a topic"),
        ("2", "also a topic")
        ))
        .toDF("qid", "query")
    
    System.out.println("found " + trainingTopics.count() + " topics in trainingTopics df")
    

    
    val queryTransform = new QueryingTransformer()
      .setTerrierProperties(
        Map(
            "terrier.home" -> "/Users/craigm/git/Terrier",
            "terrier.etc" -> "/Users/craigm/git/Terrier/etc",
            "terrier.index.path" -> "/Users/craigm/wt2g_index/index/"))
      .setInputQueryCol("query")
      .setInputQueryNumCol("qid")
      
   val r1 = queryTransform.transform(trainingTopics)
    
    
    val qrelTransform = new QrelTransformer()
        .setInputDocnoCol("docno")
        .setInputQueryNumCol("qid")
        .setOutputLabelCol("label")
        .setQrelsFile("/Users/craigm/wt2g_index/topicsqrels/small_web/qrels.trec8")
    
    val r2 = qrelTransform.transform(r1)
        
//    val pipeline = new Pipeline().setStages(
//        Array(queryTransform, qrelTransform))
//    pipeline.fit(trainingTopics)
    
  }
  
}
