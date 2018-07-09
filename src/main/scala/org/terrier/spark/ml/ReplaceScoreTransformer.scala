

package org.terrier.spark.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.util.Identifiable

/** A simple transformer that replaces the score column with the newscore column */
class ReplaceScoreTransformer(override val uid: String)
    extends Transformer {
  
  def this() = {
    this(Identifiable.randomUID("ReplaceScoreTransformer"))
  }
  
  def copy(extra: ParamMap): ReplaceScoreTransformer = {
    defaultCopy(extra)
  }
  
  def transformSchema(schema: StructType) : StructType = {
    val idx = schema.fieldIndex("newscore")
    StructType(schema.drop(idx))
  }
  
  def transform(df: Dataset[_]): DataFrame = {
    import df.sparkSession.implicits._
    df.drop("score").withColumnRenamed("newscore","score")
  }
}