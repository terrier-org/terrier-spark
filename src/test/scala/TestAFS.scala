import org.terrier.spark.ltr.AFS
import org.terrier.spark.eval.RankingMetrics2
import org.scalatest.FlatSpec

class TestAFS extends FlatSpec {
  
  "for AFS" should "checkAFS.eval usage MAP 0.5" in {
    //(String,Double,Double)
    //format qid,label,score
    val data = Array[(String,Double,Double)](
        ("qid1", 1d, 0d),//docid 1, ranked second, relevant
        ("qid1", 0d, 1d))//docid 0, ranked first, irrelevant
    println(scala.runtime.ScalaRunTime.stringOf(data))
    val iter = new org.terrier.spark.ltr.AFS().getRankingMetricsForm(data)
    println(scala.runtime.ScalaRunTime.stringOf(iter))
    assert(new org.terrier.spark.ltr.AFS().eval(data) === 0.5)   
  }
  
  "for AFS" should "checkAFS.eval usage MAP 1" in {
    //(String,Double,Double)
    //format qid,label,score
    
    //one relevant document, retrieved first
    //one non-relevant document, retrieved second
    val data = Array[(String,Double,Double)](
        ("qid1", 1d, 1d),
        ("qid1", 0d, 0.1d))
    val iter = new org.terrier.spark.ltr.AFS().getRankingMetricsForm(data)
    println(scala.runtime.ScalaRunTime.stringOf(iter))
    assert(new org.terrier.spark.ltr.AFS().eval(data) === 1)   
  }
  
  "for AFS" should "check RankingMetrics2 usage" in {
    
     var rm = new RankingMetrics2[String](List((Array("doc1","doc3"), Map( ("doc1", 1), ("doc2", 1)))));
     assert(rm.precisionAt(1) === 1);
     assert(rm.precisionAt(2) === 0.5);
     
     rm = new RankingMetrics2[String](List((Array("doc1"), Map( ("doc1", 1), ("doc2", 1)))));
     assert(rm.precisionAt(1) === 1);
     
     rm = new RankingMetrics2[String](List((Array("doc3","doc1"), Map( ("doc1", 1), ("doc2", 1)))));
     assert(rm.precisionAt(1) === 0);
     assert(rm.precisionAt(2) === 0.5);
     
     rm = new RankingMetrics2[String](List((Array("doc3","doc1"), Map( ("doc1", 1), ("doc2", 1)))));
     assert(rm.precisionAt(1) === 0);
     assert(rm.precisionAt(2) === 0.5);
     
//    val afs = new org.terrier.spark.AFS()
//    afs.eval(input)
//    val props = scala.collection.mutable.Map[String,String]()
//    props.put("terrier.index.path", "/path/to/an/index")
//    
//    val mapper = new TerrierQueryMapClient(props.toMap)
//    val (qid,resultSet) = mapper.apply(("q1", "hello"))
//    assert(qid === "q1")
//    assert(resultSet.getResultSize > 0)
  }

}