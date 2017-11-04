import org.scalatest.FlatSpec
import org.terrier.spark.TerrierQueryMapClient
import org.terrier.applications.batchquerying.QuerySource
import org.terrier.applications.batchquerying.TRECQuery
import org.terrier.spark.eval.IREvaluator
import java.util.Properties
import org.terrier.matching.daat.FatFull
import org.terrier.matching.models.InL2
import org.terrier.matching.FatFeaturedScoringMatching
import org.terrier.learning.FeaturedResultSet
import org.terrier.spark.Conversions
import org.terrier.spark.ltr.LTRFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.terrier.spark.eval.RankingMetrics2
import org.terrier.spark.eval.RankingMetrics2
import org.terrier.spark.ltr.AFS
import org.terrier.spark.ltr.QueryLabeledPoint
import org.terrier.spark.LTRPipeline


class TestTerrierPipeline extends FlatSpec {
  
  val sc = getSpark()
  
  def defaultProps() : scala.collection.mutable.Map[String,String] = {
    var rtr = scala.collection.mutable.Map[String,String]()
    rtr.put("querying.default.controls","parsecontrols:on,parseql:on,applypipeline:on,decorate:on,labels:on")
    rtr
  }
  
  def runLTRTest(method : String) : Double = {
    runLTRTest(method, defaultProps())
  }
  
  def runLTRTest(method : String, props : scala.collection.mutable.Map[String,String]) : Double = {
    props.put("terrier.home", "/Users/craigm/git/Terrier")
    props.put("terrier.etc", "/Users/craigm/git/Terrier/etc")
    props.put("fat.featured.scoring.matching.features", "WMODEL:BM25;DSM:org.terrier.matching.dsms.DFRDependenceScoreModifier")
    props.put("terrier.index.path", "/Users/craigm/wt2g_index/index")
    props.put("proximity.dependency.type", "SD")
    
    LTRPipeline.configureTerrier(props)
    
    var pipe = new LTRPipeline(
        sc,
        1, 
        method,
        "/Users/craigm/wt2g_index/topicsqrels/small_web/topics.401-450",
        null,
        props)
    
      val featRS = pipe.obtainFeatures
      displayEval(featRS)
      var model = pipe.learn(featRS)
      val mapFitted = pipe.apply(featRS, model)
      System.out.println("MAP fitted="+mapFitted)
      mapFitted
  }
  
  "Compare" should "Default and SD" in {
    var props = defaultProps()
    val map1 = runLTRTest("afs", props)
    props = defaultProps()
    props.put("querying.default.controls","parsecontrols:on,parseql:on,applypipeline:on,decorate:on,labels:on,sd:on")
    val map2 = runLTRTest("afs", props)
    System.out.println("MAPs: " + map1 + " vs " + map2)
  }
  
  "A TerrierQueryMapClient" should "can be combined with AFS" in {
    runLTRTest("afs")
  }
  
  "A TerrierQueryMapClient" should "can be combined with GBRT" in {
    runLTRTest("gbrt")
  }
  
  "A TerrierQueryMapClient" should "can be combined with RF" in {
    runLTRTest("rf")
  }
  
//    //use TRECQuery to parse wt2g topics
//    val topicsI = LTRPipeline.extractTRECTopics()
//    
//    //mapclient is a lazy configuration of Terrier
//    val mapper = new TerrierQueryMapClient(props.toMap)
//    mapper.wmodel = classOf[InL2].getName
//    mapper.matching = classOf[FatFeaturedScoringMatching].getName + ","+ classOf[FatFull].getName
//    
//    val rtrRes = topicsI.map(mapper).toList
//    displayEval(rtrRes)
//    
//    val featRtrRes = rtrRes.map{case (qid,res) => (qid, res.asInstanceOf[FeaturedResultSet])}
//    val forLearning = featRtrRes.map(new Conversions).flatten
//    System.err.println(forLearning.filter { p => p.label > 0 }.size)
//    
    
//    val sc = 
//    val rddLearning = sc.makeRDD(forLearning)
//    
////    System.out.println(sc.
////        rddLearning.toDebugString)
//    
//    val model = LTRFactory.train(sc, "rf", rddLearning, rddLearning)
//    val oFitted = model.apply(rddLearning)
//    val mapFitted = new org.terrier.spark.ltr.AFS().eval(oFitted)
//    System.out.println("MAP fitted="+mapFitted)
  //}

  

  def getSpark() = {
    val sConf = new SparkConf()
    sConf.setMaster("local")
    sConf.setAppName("tester")
    val sc = new SparkContext(sConf)
    sc
  }

  def displayEval(rtrRes: List[(String, org.terrier.matching.ResultSet)]) = {//assert(rtrRes.length >0)
    
    val evalProg = new IREvaluator(
        "/Users/craigm/wt2g_index/topicsqrels/small_web/qrels.trec8",
        "map", 1000)
    
    val evals = rtrRes.map(evalProg)
    assert(evals.length >0)
    val x = evals.map{case (qid,map) => map}
    val map = x.sum / x.length.toDouble
    evals.foreach{case (qid,map) => System.out.println(qid + " " + map)}
    System.out.println("MAP="+map)
  }

  
}