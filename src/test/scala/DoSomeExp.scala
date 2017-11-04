import org.terrier.spark.LTRPipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Hello extends App {
  def getSpark() = {
    val sConf = new SparkConf()
    sConf.setMaster("local")
    sConf.setAppName("tester")
    val sc = new SparkContext(sConf)
    sc
  }
  
  def getMap(features : Iterable[String], sc : SparkContext) = {
    var props = scala.collection.mutable.Map[String,String]()
    props.put("querying.default.controls","parsecontrols:on,parseql:on,applypipeline:on,decorate:on,labels:on")
    props.put("terrier.home", "/Users/craigm/git/Terrier")
    props.put("terrier.etc", "/Users/craigm/git/Terrier/etc")
    props.put("terrier.index.path", "/Users/craigm/wt2g_index/index")
    props.put("proximity.dependency.type", "SD")
        
    props.put("fat.featured.scoring.matching.features", features.mkString(";"))
    LTRPipeline.configureTerrier(props)
    var pipe = new LTRPipeline(
          sc,
          1, 
          method,
          "/Users/craigm/wt2g_index/topicsqrels/small_web/topics.401-450",
          null,
          props)
    
    val featRS = pipe.obtainFeatures
    var model = pipe.learn(featRS)
    var mapFitted = pipe.apply(featRS, model)
    System.out.println(method + " " + features.mkString(",") + " => " + mapFitted)
    mapFitted
  }
  
  val sc = getSpark()
  
  val featureList = Set(
      "WMODEL:BM25", 
      "WMODEL:PL2",
      "DSM:org.terrier.matching.dsms.DFRDependenceScoreModifier")
  val method = "gbrt"
  
  val baselineMap = getMap(featureList, sc)
  featureList.map( fRemoved => {
     val newFeatures = featureList - fRemoved
     val newMap = getMap(newFeatures, sc)
     (fRemoved, baselineMap - newMap)
  }).foreach(println)
  
}