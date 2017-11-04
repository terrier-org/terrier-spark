package org.terrier.spark.ltr
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

abstract class LTRModel extends Serializable {
  
  def apply(resultsets : RDD[QueryLabeledPoint]) : Iterable[(String,Double,Double)]  
  
}

case class LinearLTRModel(model : Vector) extends LTRModel
{
  def apply(resultsets : RDD[QueryLabeledPoint]) : Iterable[(String,Double,Double)] = {
    AFS.applyModel(model, resultsets);
  }
}


case class GBRTModel(model : GradientBoostedTreesModel) extends LTRModel
{
  def apply(resultsets : RDD[QueryLabeledPoint]) : Iterable[(String,Double,Double)] = {
    val rtr = resultsets.map{ case (qpoint) =>
      val prediction = model.predict(qpoint.features)
      (qpoint.qid, qpoint.label, prediction)
    };
    rtr.toLocalIterator.toIterable
  }
}

case class RFLTRModel(model : RandomForestModel) extends LTRModel
{
  def apply(resultsets : RDD[QueryLabeledPoint]) : Iterable[(String,Double,Double)] = {
    val rtr = resultsets.map{ case (qpoint) =>
      val prediction = model.predict(qpoint.features)
      (qpoint.qid, qpoint.label, prediction)
    };
    rtr.toLocalIterator.toIterable
  }
}