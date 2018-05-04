package org.terrier.spark.ltr

import org.apache.spark.rdd.RDD
import scala.annotation.switch
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy


object LTRFactory {
  
  def train(sc : SparkContext, ltrName : String, training : RDD[QueryLabeledPoint], validation : RDD[QueryLabeledPoint]) : LTRModel = {
    
    val rtr = ltrName match {
      case "afs" => {
          new LinearLTRModel(new org.terrier.spark.ltr.AFS().trainAFS(sc,training,validation))
      }
      case "gbrt" => {
        val boostingStrategy = BoostingStrategy.defaultParams("Regression")
        boostingStrategy.numIterations = 3 //use more in practice
        var model = GradientBoostedTrees.train(
              training.asInstanceOf[RDD[LabeledPoint]],
//             training.map(qp => new LabeledPoint(qp.label, qp.features)), 
             boostingStrategy)
        new GBRTModel(model)
      }     
      case "rf" => {
          val numClasses = 2
          val categoricalFeaturesInfo = Map[Int, Int]()
          val numTrees = 3 // Use more in practice.
          val featureSubsetStrategy = "auto" // Let the algorithm choose.
          val impurity = "variance"
          val maxDepth = 4
          val maxBins = 32
          //TODO set number of iterations (trees) using validation data
          val model = RandomForest.trainRegressor(
              training.asInstanceOf[RDD[LabeledPoint]],
              //training.map(qp => new LabeledPoint(qp.label, qp.features)), 
              categoricalFeaturesInfo, numTrees, featureSubsetStrategy, 
              impurity, maxDepth, maxBins)
          System.out.println(model.toDebugString)
          new RFLTRModel(model)
      }
    }
    rtr
  }
  
}