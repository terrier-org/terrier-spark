package org.terrier.spark.ml

import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.Param

/** A simple transformer that applies a (String=>String) function upon the "query" column of an input dataset */
class QueryStringTransformer (override val uid: String, fn : (String => String) ) extends Transformer {
  
  def this(fn : (String => String)) = this(Identifiable.randomUID("QueryStringTransformer" + fn.toString()), fn)  
  def copy(extra: ParamMap): QueryStringTransformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  
  final val inputQueryCol= new Param[String](this, "inputQueryCol", "The input column containing the queries. The same column is overwritten")
  setDefault(inputQueryCol -> "query")

  def transform(df: Dataset[_]): DataFrame = {
    import df.sparkSession.implicits._
    val myUDF = udf(fn)
    df.withColumn($(inputQueryCol), myUDF(col($(inputQueryCol))))
  }
}