package org.terrier.spark.ml

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.Params

/** trait for those that need to access the labels
 */
trait LabelStageTrait extends Params {
  val uid: String = "LabelStageTrait"
  
  val outputLabelCol= new Param[String](this, "outputLabelCol", "The output column containing the (relevance) label")
  def setOutputLabelCol(value: String): this.type = set(outputLabelCol, value)
  
  setDefault(outputLabelCol, "label")
  
}