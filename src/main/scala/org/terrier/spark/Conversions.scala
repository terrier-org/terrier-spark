package org.terrier.spark

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.terrier.learning.FeaturedResultSet
import org.terrier.spark.ltr.QueryLabeledPoint
import org.terrier.matching.ResultSet

object Conversions {
  
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

class Conversions extends ( ((String,FeaturedResultSet)) => Iterator[QueryLabeledPoint] ) with Serializable {
 
  def apply(input: (String,FeaturedResultSet)):Iterator[QueryLabeledPoint] = {
    val qid = input._1
    val fRes = input._2
    val numResults = fRes.getResultSize
    val numFeats = fRes.getNumberOfFeatures
    val res : Array[QueryLabeledPoint] = Array.ofDim[QueryLabeledPoint](numResults)
    val fScores : Array[Array[Double]] = Array.ofDim[ Array[Double]](numFeats)
    val labels = (fRes.getLabels).map { l => l.toDouble }
    for (f <- 0 to numFeats-1)
    {
      fScores(f) = fRes.getFeatureScores(f+1)
    }
    for (i <- 0 to numResults-1)
    {
      val feats : Array[Double] = Array.ofDim[Double](numFeats)
      for (f <- 0 to numFeats-1)
        feats(f) = fScores(f)(i)
      res(i) = new QueryLabeledPoint(qid, labels(i), Vectors.dense(feats))
    }    
    res.iterator
  }
  
}
