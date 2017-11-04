package org.terrier.spark.eval

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.Double
import org.apache.spark.ml.param.shared.HasLabelCol
import org.apache.spark.ml.param.shared.HasPredictionCol
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.shared.HasLabelCol
import org.apache.spark.ml.param.shared.HasPredictionCol




/**
 * Evaluator for ranking algorithms.
 *
 * Java users should use `RankingMetrics$.of` to create a [[RankingMetrics]] instance.
 *
 * @param predictionAndLabels an RDD of (predicted ranking, ground truth set) pairs.
 */
class RankingMetrics2[T: ClassTag](predictionAndLabels: Seq[(Array[T], Map[T,Int])]) {

  /**
   * Compute the average precision of all the queries, truncated at ranking position k.
   *
   * If for a query, the ranking algorithm returns n (n is less than k) results, the precision
   * value will be computed as #(relevant items retrieved) / k. This formula also applies when
   * the size of the ground truth set is less than k.
   *
   * If a query has an empty ground truth set, zero will be used as precision together with
   * a log warning.
   *
   * See the following paper for detail:
   *
   * IR evaluation methods for retrieving highly relevant documents. K. Jarvelin and J. Kekalainen
   *
   * @param k the position to compute the truncated precision, must be positive
   * @return the average precision at the first k ranking positions
   */
  def precisionAt(k: Int): Double = {
    require(k > 0, "ranking position k should be positive")
    val x = predictionAndLabels.map { case (pred, lab) =>
      
      if (lab.nonEmpty) {
        val n = math.min(pred.length, k)
        var i = 0
        var cnt = 0
        while (i < n) {
          val rel = lab.get(pred(i)) 
          if (rel.isDefined && rel.get > 0) {
            cnt +=1
          }
          i += 1
        }
        cnt.toDouble / k
      } else {
        //logWarning("Empty ground truth set, check input data")
        0.0
      }
    }
    x.sum / x.size.toDouble
  }

  /**
   * Returns the mean average precision (MAP) of all the queries.
   * If a query has an empty ground truth set, the average precision will be zero and a log
   * warning is generated.
   */
  lazy val meanAveragePrecision: Double = {
    val x = predictionAndLabels.map { case (pred, lab) =>
      
      val numRel = lab.filter(_._2 > 0).size
      if (numRel > 0) {
        var i = 0
        var cnt = 0
        var precSum = 0.0
        val n = pred.length
        while (i < n) {
          val rel = lab.get(pred(i)) 
          if (rel.isDefined && rel.get > 0) {
            cnt += 1
            precSum += cnt.toDouble / (i + 1)
          }
          i += 1
        }
        precSum / numRel.toDouble
      } else {
        //logWarning("Empty ground truth set, check input data")
        0.0
      }
    }
    x.sum.toDouble / x.size.toDouble
  }

  /**
   * Compute the average NDCG value of all the queries, truncated at ranking position k.
   * The discounted cumulative gain at position k is computed as:
   *    sum,,i=1,,^k^ (2^{relevance of ''i''th item}^ - 1) / log(i + 1),
   * and the NDCG is obtained by dividing the DCG value on the ground truth set. In the current
   * implementation, the relevance value is binary.

   * If a query has an empty ground truth set, zero will be used as ndcg together with
   * a log warning.
   *
   * See the following paper for detail:
   *
   * IR evaluation methods for retrieving highly relevant documents. K. Jarvelin and J. Kekalainen
   *
   * @param k the position to compute the truncated ndcg, must be positive
   * @return the average ndcg at the first k ranking positions
   */
  def ndcgAt(k: Int): Double = {
    require(k > 0, "ranking position k should be positive")
    val x = predictionAndLabels.map { case (pred, lab) =>
      
      val numRel = lab.filter(_._2 > 0).size
      if (numRel > 0) {
        val labSetSize = lab.size
        val n = math.min(math.max(pred.length, labSetSize), k)
        var maxDcg = lab.toSeq.sortBy(-1 * _._2).zipWithIndex.map{ case (((docid, rel), rank)) => rel / math.log(rank + 2) }.sum
        
        var dcg = 0.0
        var i = 0
        while (i < n) {
          val rel = lab.get(pred(i)) 
          if (i < pred.length && rel.isDefined && rel.get > 0) {
            val gain = rel.get / math.log(i + 2)
            dcg += gain
          }
//        if (i < labSetSize) {
//            maxDcg += gain
//        }
          i += 1
        }
        dcg / maxDcg
      } else {
        //logWarning("Empty ground truth set, check input data")
        0.0
      }
    }
    x.sum.toDouble / x.size.toDouble
  }

}