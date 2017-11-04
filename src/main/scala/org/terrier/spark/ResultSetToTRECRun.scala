package org.terrier.spark

import org.terrier.matching.ResultSet

class ResultSetToTRECRun extends ( ((String,ResultSet)) => (String) ) with Serializable {
  def apply(input: (String,ResultSet)) : String = {
    val qid = input._1
    val res = input._2
    val numResults = res.getResultSize
    val docnos = res.getMetaItems("docno")
    val scores = res.getScores
    var rtr = ""
    for (i <- 0 to numResults-1)
    {
      rtr += qid + " Q0 " + docnos(i) + " " + i + " " + scores(i) + "\n";
    }
    rtr
  }
}