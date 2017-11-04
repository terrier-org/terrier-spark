package org.terrier.spark

import java.util.Properties
import org.terrier.applications.batchquerying.QuerySource
import org.terrier.applications.batchquerying.TRECQuery
import org.terrier.matching.models.InL2
import org.terrier.matching.FatFeaturedScoringMatching
import org.terrier.matching.daat.FatFull
import org.apache.spark.SparkContext
import org.terrier.learning.FeaturedResultSet
import org.terrier.spark.ltr.LTRFactory
import org.terrier.spark.ltr.LTRModel
import org.terrier.spark.ltr.AFS



class LTRPipeline(
    _sc : SparkContext,
    _numF : Int, 
    _ltrMethod : String, 
    _topics : String,
    _qrels : String,
    _props : scala.collection.Map[String,String]) {
  
  val sc : SparkContext = _sc;
  val numFolds : Int = _numF;
  val ltr : String = _ltrMethod;
  val topics  = _topics;
  val qrels = _qrels;
  val measure = "map"
  val wmodel = classOf[InL2].getName
  val matching = classOf[FatFeaturedScoringMatching].getName + ","+ classOf[FatFull].getName
  
  val props = _props;
  
  def init = {
    LTRPipeline.configureTerrier(props)
  }
  
  def obtainFeatures = {
    //use TRECQuery to parse wt2g topics
    val topicsI = LTRPipeline.extractTRECTopics(topics)
    //mapclient is a lazy configuration of Terrier
    val mapper = new TerrierQueryMapClient(props.toMap)
    mapper.wmodel = wmodel
    mapper.matching = matching
    
    val rtrRes = topicsI.map(mapper).toList
    val featRtrRes = rtrRes.map{case (qid,res) => (qid, res.asInstanceOf[FeaturedResultSet])}
    featRtrRes
  }
  
  def learn(featRtrRes : Seq[(String, FeaturedResultSet)] )  = {
    
    val asPoints = featRtrRes.map(new Conversions).flatten
    val rddAllQueries = sc.makeRDD(asPoints)//.sample(withReplacement, fraction, seed)
    
    val model = LTRFactory.train(
        sc, 
        ltr, 
        rddAllQueries, 
        rddAllQueries)
    model
  }
  
  def apply(featRtrRes : Seq[(String, FeaturedResultSet)], model : LTRModel) = {
    val res = model.apply(sc.makeRDD(featRtrRes.map(new Conversions).flatten))
    new AFS().eval(res)
  }
  
}

object LTRPipeline {
  def configureTerrier(props : scala.collection.Map[String,String]) = {
//    val props = scala.collection.mutable.Map[String,String]()
//    props.put("terrier.home", "/Users/craigm/git/Terrier")
//    props.put("terrier.etc", "/Users/craigm/git/Terrier/etc")
//    props.put("fat.featured.scoring.matching.features", "FILE")
//    props.put("fat.featured.scoring.matching.features.file", "features.list")
//    //props.put("stopwords.filename", "/Users/craigm/git/Terrier/share/stopword-list.txt")
//    props.put("querying.default.controls","parsecontrols:on,parseql:on,applypipeline:on,decorate:on,labels:on")
//    props.put("terrier.index.path", "/Users/craigm/wt2g_index/index")
//    props.put("proximity.dependency.type", "SD")
    
    //val x : Option[String] = new Option[String]
  
    
    //force terrier init now
    val tProps : Properties  = new Properties();
    for ((k,v) <- props) tProps.setProperty(k, v)
    org.terrier.utility.ApplicationSetup.bootstrapInitialisation(tProps)
    props
  }

  //use TRECQuery to parse wt2g topics
  def extractTRECTopics(topicsFile : String) = {
    val topicsSource : QuerySource = new TRECQuery(topicsFile)

    //resort to java to get a scala iterator of qids and queries
    var topics = new java.util.ArrayList[(String,String)]()
    while(topicsSource.hasNext()) {
      val topic = topicsSource.next
      val id = topicsSource.getQueryId
      topics.add((id,topic))
    }
    assert(topics.size() > 0)
    val topicsI = scala.collection.JavaConversions.asScalaIterator(topics.iterator())
    topicsI
  }
}