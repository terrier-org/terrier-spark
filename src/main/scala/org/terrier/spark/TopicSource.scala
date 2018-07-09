package org.terrier.spark

import java.util.Properties
import org.terrier.applications.batchquerying.QuerySource
import org.terrier.applications.batchquerying.TRECQuery
import org.terrier.applications.batchquerying.SingleLineTRECQuery

/** this object is useful for extracting topics from files */
object TopicSource {
  def configureTerrier(props : scala.collection.Map[String,String]) = {
    //force terrier init now
    val tProps : Properties  = new Properties();
    for ((k,v) <- props) tProps.setProperty(k, v)
    org.terrier.utility.ApplicationSetup.bootstrapInitialisation(tProps)
    props
  }
  
  //use SingleLineTRECQuery to parse topics file
  def extractSingleLineTopics(topicsFile : String) = {
    val topicsSource : QuerySource = new SingleLineTRECQuery(topicsFile)

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
  
  //use TRECQuery to parse TREC formatted topics
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