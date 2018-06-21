package org.terrier.spark

import scala.collection.JavaConverters._

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.terrier.learning.FeaturedResultSet
import org.terrier.matching.ResultSet
import org.terrier.querying.ScoredDocList


object Conversions {
  
  def mapScoredDocList(res : ScoredDocList, maxResults : Int) : Iterable[(String, Int, Double, Int)] =
  {
    require( (res.getMetaKeys contains "docno") || res.size == 0, "ScoredDocList from Terrier must provide docno metadata.")
    val numResults = Math.min(res.size, maxResults)
    res
      .asScala
      .slice(0, numResults -1)
      .zipWithIndex.map{ 
        case (d, i)  => (d.getMetadata("docno"), d.getDocid, d.getScore, i)
        }
  }
  
  def mapResultSet(res: ResultSet, maxResults : Int) : Iterable[(String, Int, Double, Int)] =
  {
    require(res.hasMetaItems("docno") || res.getResultSize == 0, "ResultSet from Terrier must provide docno metadata. "
        +"Perhaps decorate:on needs to be set as a control")
    val numResults = Math.min(res.getResultSize, maxResults)
    val rtr = Array.ofDim[(String, Int, Double, Int)](numResults)
    for (i <- 0 to numResults-1)
    {
      val row = (res.getMetaItems("docno")(i), res.getDocids()(i), res.getScores()(i), i)
      rtr(i) = row
    }
    rtr
  }
}

