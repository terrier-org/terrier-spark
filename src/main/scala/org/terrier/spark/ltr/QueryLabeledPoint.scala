package org.terrier.spark.ltr

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint

class QueryLabeledPoint(_qid : String, label : Double, values : Vector) 
  extends LabeledPoint(label, values) {
  val qid = _qid
}

object QueryLabeledPoint {
  
  def display(iter : Iterable[QueryLabeledPoint]) = {
    
    iter.foreach { qlp => System.out.println(qlp.label +" qid:"+qlp.qid + " " + qlp.features.toString() ) }
    
  }
  
}