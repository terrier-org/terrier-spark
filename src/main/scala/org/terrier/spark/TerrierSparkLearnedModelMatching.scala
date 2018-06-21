package org.terrier.spark

import org.terrier.matching.Matching
import org.terrier.structures.Index
import org.apache.spark.ml.Model
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/** this allows a randomforest model developed in spark to 
 *  be used directly in Terrier */
class TerrierSparkLearnedModelMatching(_index : Index , _parent : Matching) 
extends org.terrier.matching.LearnedModelMatching(_index, _parent)
{
  val model = RandomForestRegressionModel.load("my.model")
  val predColumn = model.getPredictionCol
  
  val spark = SparkSession
   .builder()
   .getOrCreate()
  
  def applyModel(numDocs: Int, in_scores: Array[Double], F: Int, features: Array[Array[Double]], outscores: Array[Double]) : Unit = {
    assert(model.numFeatures == F)
    
    import spark.implicits._
    val featuresDS = features.toSeq.toDS()
    val results = model.transform( featuresDS )
    
    val rtr = results.collect();
    for(i <- 0 to numDocs)
      outscores(i) = rtr(i).getAs(predColumn)
  }
}