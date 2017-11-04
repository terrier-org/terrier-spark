package org.terrier.spark

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.terrier.learning.FeaturedResultSet
import org.terrier.spark.ltr.QueryLabeledPoint

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