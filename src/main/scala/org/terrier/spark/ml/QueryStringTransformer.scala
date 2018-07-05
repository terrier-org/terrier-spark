package org.terrier.spark.ml

import org.apache.spark.sql._
import org.apache.spark.ml._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.ParamMap

/** A simple transformer that applies a String=>String function upon the "query" column of an input dataset */
class QueryStringTransformer (override val uid: String, fn : (String => String) ) extends Transformer {
  
  def this(fn : (String => String)) = this(Identifiable.randomUID("MRFQueryTransformer" + fn.toString()), fn)  
  def copy(extra: ParamMap): QueryStringTransformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  
  def transform(df: Dataset[_]): DataFrame = {
    import df.sparkSession.implicits._
    val myUDF = udf(fn)
    df.withColumn("query", myUDF(col("query")))
  }
}