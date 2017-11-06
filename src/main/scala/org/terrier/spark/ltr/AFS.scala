package org.terrier.spark.ltr

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
import org.apache.spark.SparkContext
import com.nr.RealValueFun
import com.nr.min.Amebsa
import org.netlib.util.intW
import org.apache.commons.collections4.map.LRUMap
import org.terrier.spark.eval.RankingMetrics2
import com.github.bruneli.scalaopt.core._
import derivativefree.NelderMead._

import com.github.bruneli.scalaopt.core.ObjectiveFunction
import com.github.bruneli.scalaopt.core.derivativefree.NelderMeadConfig

object AFS {
  //makes the dotproduct of two vector
  def dotProduct(v1 : Vector, v2 : Vector) : Double = {
    var total : Double = 0
    v1.foreachActive( (fid,weight) => total = total + weight * v2.apply(fid) )
    total
  }
  
  //combines to sparse vectors
  def combine(v1:SparseVector, v2:SparseVector):SparseVector = {
    val size = v1.size + v2.size
    val maxIndex = v1.size
    val indices = v1.indices ++ v2.indices.map(e => e + maxIndex)
    val values = v1.values ++ v2.values
    new SparseVector(size, indices, values)
  }
  
  
  //get a model that we can evaluate
  def applyModel(model : Vector, resultsets : RDD[QueryLabeledPoint] ) : Iterable[(String,Double,Double)] = {
    val rtr = resultsets.map(x => (x.qid, x.label, dotProduct(model, x.features)));
    rtr.toLocalIterator.toSeq
  }
}

class AFS extends Serializable {
  
  
  //format qid,label,score
  def getRankingMetricsForm(input : Iterable[(String,Double,Double)]) : Seq[(Array[Int], Map[Int,Int])] = {
    val groupedByQuery = input.groupBy(_._1).toSeq
    val iterd = groupedByQuery.map{ 
      case (qid,thingIterator) => 
        
        val sortedByDescScore = thingIterator.toArray.sortBy(p => -1d* p._3).zipWithIndex
        var labelArray = sortedByDescScore.map{ case(p,docid) => (docid, p._2.toInt) }
        var scoreArray = sortedByDescScore.map{ case(p,docid) => docid } 
        (scoreArray, labelArray.toMap)
    }
    iterd
  }
  
  //format qid,label,score
  def eval(input : Iterable[(String,Double,Double)]) : Double = {
    val iterd = getRankingMetricsForm(input)
    val metrics = new RankingMetrics2[Int](iterd)
    metrics.meanAveragePrecision
    //ndcgAt(1000)
  }
  
  //qid, label, current model, feature
  //returns feature weight and NDCG, that maximises NDCG
  def maximise_iter(input : Iterable[(String,Double,Double,Double)]) : (Double,Double) = {
    var maxW = -1;
    var maxEval = -1;
    val range = 0.0 to 1.0 by 0.01;
    val weight2eval = range.map{ 
      case (fWeight) => 
        (fWeight, eval(input.map{ case ( qid, label, currentScore, feature ) => (qid, label, currentScore + fWeight * feature )}))
        //(fWeight, eval(input.map{ case ( qid, label, currentScore, feature ) => (qid, label, fWeight * currentScore + (1d -fWeight) * feature )}))
    };
    weight2eval.maxBy(_._2)    
  }
  
  //uses simulated annealing from Numerical Recipes in C to maximise the effectiveness
  def maximise_sa(input : Iterable[(String,Double,Double,Double)]) : (Double,Double) = {
    
    class AFS_opt_funk extends ObjectiveFunction
    {
      val min = 0d;
      val max = 1000d;
      //val oob_eval = 0d;
      val recent : LRUMap[Double,Double] = new LRUMap[Double,Double](1000)
      def apply(x: Variables) : Double = {
        val fWeight = x(0)
        //short circuit large weights
        if ((fWeight > max) || (fWeight < min)) {
          //println(s"fWeight = $fWeight OOB");
          return(Math.pow(Math.abs(fWeight - 0), 2));
          //return(oob_eval)
        }
        if (fWeight.isNaN()) {
          //println(s"fWeight NaN");
          return(Double.MaxValue)
        }
        //check the LRU cache
        val rtr = if (recent.containsKey(fWeight)) {
          recent.get(fWeight)          
        }else{
          //actually evaluate the weight of the feature
          val out = eval(input.map{ case ( qid, label, currentScore, feature ) => (qid, label, currentScore + fWeight * feature )})
          recent.put(fWeight, out)
          out
        }
        //println(s"$fWeight => $rtr");
        -1d * rtr
      }
    }
    //how effective is the current model?
    val minimumNDCG = eval(input.map{ case ( qid, label, currentScore, feature ) => (qid, label, currentScore)})
    
    val func = new AFS_opt_funk()
    val config = new NelderMeadConfig(tol = 1e04, maxIter = 100)
    val tuned = minimize(func, Vector(0))(config)
    
//    //first parameter is starting solution
//    //second parameter is how big to explore
//    val amb1: Amebsa = new Amebsa(Array(0d), 0.0001d, new AFS_opt_funk(), 1.0e-4)
//    var temperature=10.0;
//    var iter : intW = new intW(0);
//    iter.`val` = 100;
//    var i = 0;
//    var test = false;
//    
//    //main annealing loop
//    while(i < 200 && ! test)
//    {
//      //println(s"Annealing iteration $i temp $temperature");
//      test = amb1.anneal(iter, temperature)
//      iter.`val` = 100;
//      temperature *= 0.8;
//      i=i+1
//    }
    
    val bestWeight = tuned.get(0)
    val bestNDCG = -1d * func.apply(Vector(bestWeight))
    if (bestNDCG > minimumNDCG)
    {  
      (bestWeight,bestNDCG)
    } else {
      (0d, minimumNDCG)
    }
  }
  
  
  
  //measure the effectiveness of each feature individually
  def measureFeatureEffectiveness(sc : SparkContext, training : RDD[QueryLabeledPoint] ) = {
    
    val numF = training.first.features.size
    var remainingFeatures = 0 to numF-1 toSet
    val indpFeaturesPerformances = sc.parallelize(remainingFeatures.toList).cartesian(training).map{
      case (fid, qpoint) => 
         (fid, qpoint.qid, qpoint.label, qpoint.features.apply(fid))
    }.groupBy(_._1).map{
      case (fid, iterator) => 
        val reformIterator = iterator.map( x => (x._2, x._3, x._4) )
        val ndcg = eval(reformIterator)
        println(s"Feature $fid => $ndcg")
        (fid, ndcg)
    }.toLocalIterator
    indpFeaturesPerformances
  }
  
  //main method : make a linear model
  def trainAFS(sc : SparkContext, training : RDD[QueryLabeledPoint], validation : RDD[QueryLabeledPoint] ) : Vector = {
    val numF = training.first.features.size
    var remainingFeatures = 0 to numF-1 toSet
    
    //startup phase: measure the effectiveness of each feature
    println(s"Startup iteration, measuring effectiveness of each of the $numF features")
    val indpFeaturesPerformances = measureFeatureEffectiveness(sc, training)
    println(s"Startup iteration, empirical feature performances:")
    
    val bestFeature = indpFeaturesPerformances.maxBy(_._2);
    remainingFeatures = remainingFeatures - bestFeature._1
    var currentModel = Vectors.sparse(1, Array(bestFeature._1), Array(1d)).asInstanceOf[SparseVector]
    
   
    var bestTraining = bestFeature._2 
    var bestValidation = eval(AFS.applyModel(currentModel, validation))    
    println(s"Startup iteration, selected ${bestFeature._1} with training NDCG $bestTraining, validation NDCG $bestValidation")
    var bestValidationModel = currentModel.copy
    
    //parameter : how much do things need to improve by?
    val epsilon = 0d;
    
    
    var iter = 1;
    var stillImproving = true
    while (iter < numF -1 && stillImproving)
    {
        //cart product of feature ids with datasets.
        val cartProduct = sc.parallelize(remainingFeatures.toList).cartesian(training).map{ 
            case (fid, qpoint) => 
              (fid, qpoint.qid, qpoint.label, AFS.dotProduct(currentModel, qpoint.features), qpoint.features.apply(fid))
        }
        
        //maximise the NDCG of each of the features in combination with current model
        val weightMeasures = cartProduct.groupBy(_._1).map{ 
           case (fid, iterator) => 
              val reformIterator = iterator.map( x => (x._2, x._3, x._4, x._5));
              val (weight,ndcg) = maximise_sa(reformIterator)
              println(s"Feature $fid best NDCG $ndcg for weight $weight")
              (fid,weight,ndcg)
        }.cache();
        
        //how many features improved over training NDCG of current model?
        val possibleCandidatesCount = weightMeasures.filter(_._3 > (bestTraining + epsilon)).count
        println(s"Iteration $iter, $possibleCandidatesCount features improved over current model")
        val selected = weightMeasures.toLocalIterator.maxBy(_._3)
        
        println(s"Iteration $iter, best feature tuple is $selected")
        //identify the new feature to add to the current model
        if (selected._3 > bestTraining && selected._3 - bestTraining > epsilon) {
          println(s"Improved over previous best of $bestTraining");
          bestTraining = selected._3
          remainingFeatures = remainingFeatures - selected._1
          currentModel = AFS.combine(currentModel, Vectors.sparse(1, Array(selected._1), Array(selected._2)).asInstanceOf[SparseVector] );
          stillImproving = true;
          println(s"New model is $currentModel")
          
          //check if the new model improved validation performance, and if so, accept
          val newValidationNDCG = eval(AFS.applyModel(currentModel, validation))
          if (newValidationNDCG > bestValidation + epsilon)
          {
            println(s"Improved validation performance to $newValidationNDCG over previous best of $bestValidation");
            bestValidationModel = currentModel.copy
            bestValidation = newValidationNDCG
          }
          
        } else {
          println(s"No improvement on training data over $bestTraining")
          stillImproving = false
        }
        iter = iter +1      
    }
    
    val featureCount = bestValidationModel.numActives
    println(s"Selecting validation model with $featureCount, performance $bestValidation");
    bestValidationModel
  }
  
}