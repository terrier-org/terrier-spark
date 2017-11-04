package org.terrier.spark.eval

import scala.collection.JavaConversions._
import org.terrier.evaluation.InMemoryEvaluationProcessor
import org.terrier.matching.ResultSet
import org.lemurproject.ireval.RetrievalEvaluator.Document

class IREvaluator(qrelsFile : String, measure : String, cutoff : Int) extends InMemoryEvaluationProcessor(qrelsFile) 
  with ( ((String,ResultSet)) => (String,Double) )
{
  val allEvals = allQueryEvaluators
  val trecEvalSorter = trecEvalDocumentComparator
  
  def apply( input : (String,ResultSet) ) : (String,Double) = {
    val qid = input._1
    val res = input._2
    val numRes = res.getResultSize
    val allEvaluator = allEvals
    val evaluator = allEvaluator.get(qid)
    val docnos = res.getMetaItems("docno")
    val scores = res.getScores
    //we need to use a Java ArrayList for this, to be compatible with IREval
    var ranking : java.util.List[Document] = new java.util.ArrayList[Document](numRes)
    for(i <- 0 to numRes-1)
    {
      val rankedDoc = new Document(docnos(i), i+1, scores(i))
      ranking.add(rankedDoc)
    }
    
    //we resort the ranking as per trec_eval's default sort order, and then apply the cutoff
    java.util.Collections.sort(ranking, trecEvalSorter);
    var i : Int = 1;
    for(d <- ranking) {      
      d.rank = i
      i = i+1
    }
    if (ranking.size() > cutoff)
      ranking = ranking.subList(0, cutoff -1);
    //calculate the measure
    evaluator.evaluate(ranking);
    val rtr = measure match {
      case "ndcg" => evaluator.normalizedDiscountedCumulativeGain
      case "map" => evaluator.averagePrecision
      case "p" => evaluator.precision(cutoff)
      case "recip_rank"|"mrr" => evaluator.reciprocalRank
      case "r-Prec" => evaluator.rPrecision
    }
    (qid, rtr)
  }
}