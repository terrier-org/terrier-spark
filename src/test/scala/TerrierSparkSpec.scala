import org.scalatest.FlatSpec
import org.terrier.spark.TerrierQueryMapClient
import org.terrier.applications.batchquerying.QuerySource
import org.terrier.applications.batchquerying.TRECQuery
import org.terrier.querying.IndexRef
import org.terrier.querying.Request


class TerrierSparkSpec extends FlatSpec {
  
  val local_indexref = IndexRef.of("/Users/craigm/wt2g_index/index/data.properties")
  val remote_indexref = IndexRef.of("http://demos.terrier.org/cw09b/")
  
  "A TerrierQueryMapClient" should "return results" in {
    val props = scala.collection.mutable.Map[String,String]()
    props.put("terrier.home", "/Users/craigm/git/Terrier")
    props.put("terrier.etc", "/Users/craigm/git/Terrier/etc")
    val mapper = new TerrierQueryMapClient(local_indexref, props.toMap)
    val srq = mapper.apply(("q1", "hello"))
    val numRes = srq.asInstanceOf[Request].getResultSet.getResultSize 
    System.out.println(numRes)
    assert(numRes > 0)
  }
  
   "A remote TerrierQueryMapClient" should "return results" in {
    val props = scala.collection.mutable.Map[String,String]()
    props.put("terrier.home", "/Users/craigm/git/Terrier")
    props.put("terrier.etc", "/Users/craigm/git/Terrier/etc") 
    val mapper = new TerrierQueryMapClient(remote_indexref, props.toMap)
    val srq = mapper.apply(("q1", "indri"))
    assert(srq.getResults.size > 0)
  }
}