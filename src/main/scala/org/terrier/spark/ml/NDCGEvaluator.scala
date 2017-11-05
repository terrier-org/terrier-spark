package org.terrier.spark.ml

import org.apache.spark.ml.param.ParamMap
import org.terrier.spark.eval.RankingMetrics2
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.sql.Row

class NDCGEvalutor(cutoff : Int) extends Evaluator
  {
    val uid: String = "NDCGEvaluator"
    
    def copy(extra: ParamMap): NDCGEvalutor = {
      defaultCopy(extra)
    }
    
    def evaluate(dataset: Dataset[_]): Double = {
      import dataset.sqlContext.implicits._
      import org.apache.spark.sql.functions._
      val xdd = dataset.coalesce(1).orderBy(asc("qid"), desc("score")).
        select("qid", "rank", "score", "label").map{
          case Row(qid: String, rank: Int, score: Double, label : Int) =>
          (qid,rank,score,label)
        }.rdd
      val xee = xdd.groupBy(_._1)
      val xff = xee.map{case (qid : String, docs : Iterable[(String,Int,Double,Int)])  =>
        (docs.map{item => item._2}.toArray, docs.map{item => (item._2 -> item._4)}.toMap)          
      }
      val xgg = xff.toLocalIterator.toSeq
      new RankingMetrics2[Int](xgg).ndcgAt(cutoff)
    }
  }