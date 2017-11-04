import org.scalatest.FlatSpec
import org.terrier.spark.TerrierQueryMapClient
import org.terrier.applications.batchquerying.QuerySource
import org.terrier.applications.batchquerying.TRECQuery


class TerrierSparkSpec extends FlatSpec {
  "A TerrierQueryMapClient" should "not return 0 results" in {
    val props = scala.collection.mutable.Map[String,String]()
    props.put("terrier.home", "/Users/craigm/git/Terrier")
    props.put("terrier.etc", "/Users/craigm/git/Terrier/etc")
    //props.put("stopwords.filename", "/Users/craigm/git/Terrier/share/stopword-list.txt")
    props.put("querying.default.controls","parsecontrols:on,parseql:on,applypipeline:on")
    props.put("terrier.index.path", "/Users/craigm/wt2g_index/index")
    
    val mapper = new TerrierQueryMapClient(props.toMap)
    val (qid,resultSet) = mapper.apply(("q1", "hello"))
    assert(qid === "q1")
    assert(resultSet.getResultSize > 0)
  }
}