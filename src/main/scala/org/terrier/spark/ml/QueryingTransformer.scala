package org.terrier.spark.ml

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.DataFrame
import org.terrier.spark.TerrierQueryMapClient
import org.terrier.matching.ResultSet
import org.terrier.matching.FatFeaturedScoringMatching
import org.terrier.matching.daat.FatFull
import org.apache.spark.ml.linalg.Vector
import org.terrier.learning.FeaturedResultSet
import org.apache.spark.ml.linalg.Vectors
import org.terrier.matching.FatFeaturedScoringMatching
import org.terrier.matching.daat.FatFull
import org.apache.spark.ml.PipelineStage

class FeaturesQueryingTransformer(override val uid: String) extends QueryingTransformer(uid)
{
  final val retrievalFeatures = new Param[Seq[String]](this, "retrievalFeatures", "The names of features to use")
  
  def this() = {
    this(Identifiable.randomUID("FeaturesQueryingTransformer"))
  }
  
  def setRetrievalFeatures(feats: Seq[String]): this.type = set(retrievalFeatures, feats)
  setDefault(retrievalFeatures -> List())
  
  override def transformSchema(schema: StructType): StructType = {
    var newSchema = super.transformSchema(schema)
    newSchema = newSchema.add("features", VectorType)
    newSchema
  }
  
   override def getTerrier() = {
     val props = get(localTerrierProperties).get
     val props2 = props ++ Map("fat.featured.scoring.matching.features" -> get(retrievalFeatures).get.mkString(";"))
     set(localTerrierProperties, props2)
     val terrier = super.getTerrier
     terrier.matching = classOf[FatFeaturedScoringMatching].getName + ","+ classOf[FatFull].getName
     terrier
   }
   
  def mapResultSetFR(res: FeaturedResultSet) : Iterable[(String, Int, Double, Int, Vector)] = 
  {
    val numResults = res.getResultSize
    val numFeats = res.getNumberOfFeatures
    val featNames = res.getFeatureNames()
    val rtr = Array.ofDim[(String, Int, Double, Int, Vector)](numResults)
    for (i <- 0 to numResults-1)
    {
      val feats = featNames.map(fname => res.getFeatureScores(fname)(i))
      //val feats = (0 to numFeats-1).toArray.map(fid => res.getFeatureScores(fid)(i))
      val row = (res.getMetaItems("docno")(i), res.getDocids()(i), res.getScores()(i), i, Vectors.dense(feats))
      rtr(i) = row
    }
    rtr
  }
  
  override def transform(df: Dataset[_]): DataFrame = {
    import df.sparkSession.implicits._

    System.out.println("Querying for "+ df.count() + " queries")
    
    def getRes2(qid : String, query : String) : Iterable[(String, Int, Double, Int, Vector)] = {
      mapResultSetFR(getTerrier.apply((qid,query))._2.asInstanceOf[FeaturedResultSet])
    }
    
    val newDF = df.select($(inputQueryNumCol), $(inputQueryCol)).as[(String,String)]
    
    val resDF = newDF.flatMap{ 
      case (qid, query) => getRes2(qid,query).map( x=> (qid, x._1, x._2, x._3, x._4, x._5))
    }.toDF($(inputQueryNumCol), "docno", "docid", "score", "rank", "features")
    
    val rtr = df.join(resDF, $(inputQueryNumCol))
    System.out.println("Got for "+ rtr.count() + " results total")
    rtr
  }
}

class QueryingTransformer(override val uid: String) extends Transformer with QueryingPipelineStage {

  def this() = {
    this(Identifiable.randomUID("QueryingTransformer"))
  }
  
  def copy(extra: ParamMap): QueryingTransformer = {
    defaultCopy(extra)
  }
}

trait QueryingPipelineStage extends PipelineStage {
  
  final val inputQueryCol= new Param[String](this, "inputQueryCol", "The input column containing the queries")
  final val inputQueryNumCol= new Param[String](this, "inputQueryNumCol", "The input column containing the queries")
  final val sampleModel = new Param[String](this, "sampleModel", "The sample weighting model")
  final val localTerrierProperties = new Param[Map[String,String]](this, "localTerrierProperties", "TR properties")
  //TODO this should perhaps be sent through as a property/control for Terrier
  final val maxResults = new Param[Int](this, "maxResults", "Max number of results for QueryingTransformer to render for each query")

  setDefault(localTerrierProperties -> Map())
  setDefault(sampleModel -> "InL2")
  setDefault(maxResults -> 1000)
  setDefault(inputQueryCol -> "query")
  setDefault(inputQueryNumCol -> "qid")

  var terrier : Option[TerrierQueryMapClient] = None
  
  def getTerrier() = {
    if (terrier.isEmpty)
    {
      terrier = Some( new TerrierQueryMapClient(
        getTerrierProperties ))
    }
    val rtr = terrier.get
    rtr.wmodel = $(sampleModel)
    rtr
  }
  
  def getTerrierProperties() = {
   get(localTerrierProperties).get 
  }
  
  def setInputQueryCol(value: String): this.type = set(inputQueryCol, value)
  def setInputQueryNumCol(value: String): this.type = set(inputQueryNumCol, value)
  def setSampleModel(value : String): this.type = set(sampleModel, value)
  def setMaxResults(K : Int): this.type = set(maxResults, K)
  def setTerrierProperties(value : Map[String,String]): this.type = set(localTerrierProperties, value)
  
  override def transformSchema(schema: StructType): StructType = {
     // Check that the input query type is a string
    var idx = schema.fieldIndex($(inputQueryCol))
    var field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type of ${inputQueryCol} ${field.dataType} did not match input type StringType")
    }
    
    // Check that the input query num type is a string
    idx = schema.fieldIndex($(inputQueryNumCol))
    field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type of ${inputQueryNumCol} ${field.dataType} did not match input type StringType")
    }
    
    // Add the return field
    schema
      .add(StructField("docno", StringType, false))
      .add(StructField("docid", IntegerType, false))
      .add(StructField("score", DoubleType, false))    
      .add(StructField("rank", IntegerType, false))
  }
  
  def mapResultSet(res: ResultSet) : Iterable[(String, Int, Double, Int)] = 
  {
    val numResults = Math.min(res.getResultSize, $(maxResults))
    val rtr = Array.ofDim[(String, Int, Double, Int)](numResults)
    for (i <- 0 to numResults-1)
    {
      val row = (res.getMetaItems("docno")(i), res.getDocids()(i), res.getScores()(i), i)
      rtr(i) = row
    }
    rtr
  }
  
  def transform(df: Dataset[_]): DataFrame = {
    import df.sparkSession.implicits._

    System.out.println("Querying for "+ df.count() + " queries")
    
    def getRes2(qid : String, query : String) : Iterable[(String, Int, Double, Int)] = {
      mapResultSet(getTerrier.apply((qid,query))._2)
    }
    
    val newDF = df.select($(inputQueryNumCol), $(inputQueryCol)).as[(String,String)]
    
    val resDF = newDF.flatMap{ 
      case (qid, query) => getRes2(qid,query).map( x=> (qid, x._1, x._2, x._3, x._4) )   
    }.toDF($(inputQueryNumCol), "docno", "docid", "score", "rank")
    
    val rtr = df.join(resDF, $(inputQueryNumCol))
    System.out.println("Got for "+ rtr.count() + " results total")
    rtr
  }

  
}