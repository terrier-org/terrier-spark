package org.terrier.spark.ml.eval

object Measure extends Enumeration {
  val NDCG, MAP, P = Value
  
  def fromString(name : String) = {
    name match {
      case "ndcg" => Measure.NDCG
      case "map" => Measure.MAP
    }
  }
}