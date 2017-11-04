package org.terrier.spark.ml

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.PipelineStage

trait NeedQrels extends PipelineStage {
  final val qrelsFile = new Param[String](this, "qrelsFile", "The location of the qrels file")
  def setQrelsFile(value: String): this.type = set(qrelsFile, value)
}

class QrelTransformer(override val uid: String) extends Transformer with NeedQrels {
  
  def this() = {
    this(Identifiable.randomUID("QrelTransformer"))
  }
  
  
  final val outputLabelCol= new Param[String](this, "outputLabelCol", "The output column containing the (relevance) label")
  final val inputQueryNumCol= new Param[String](this, "inputQueryNumCol", "The input column containing the queries")
  final val inputDocnoCol= new Param[String](this, "inputDocnoCol", "The input column containing the docnos")
  
  def copy(extra: ParamMap): QrelTransformer = {
    defaultCopy(extra)
  }
  
  def setOutputLabelCol(value: String): this.type = set(outputLabelCol, value)
  def setInputDocnoCol(value: String): this.type = set(inputDocnoCol, value)
  def setInputQueryNumCol(value: String): this.type = set(inputQueryNumCol, value)
  
  
  setDefault(outputLabelCol, "label")
  setDefault(inputQueryNumCol, "qid")
  setDefault(inputDocnoCol, "docno")
  
  
   override def transformSchema(schema: StructType): StructType = {
     // Check that the input query type is a string
    // Check that the input query num type is a string
    System.out.println("input schema: "+ schema.toString())
    
    var idx = schema.fieldIndex($(inputQueryNumCol))
    var field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type of ${inputQueryNumCol} ${field.dataType} did not match input type StringType")
    }
    
    idx = schema.fieldIndex($(inputDocnoCol))
    
    // Add the return field
    val finalSchema = schema.add(StructField($(outputLabelCol), IntegerType, false))
    System.out.println("output schema: "+ finalSchema.toString())
    finalSchema
  }
  
  def transform(df: Dataset[_]): DataFrame = {
    import df.sparkSession.implicits._
    
    val qrelLines = df.sparkSession.read.textFile(get(qrelsFile).get)
    val qrelDF = qrelLines
      .map(_.split("\\s+"))
      .map(parts => (parts(0), parts(1), parts(2), Integer.parseInt(parts(3)) ))
      .toDF("qid", "iter", "docno", $(outputLabelCol)).as("qrels")
    
    System.out.println("We have " + qrelDF.count() + " qrels")
    df.as("res")
      .join(qrelDF, df($(inputQueryNumCol)) === qrelDF("qid") && df($(inputDocnoCol)) === qrelDF("docno"), "left_outer")
      .drop($"qrels.docno").drop($"qrels.qid").drop($"qrels.iter")
      .na.fill(0, Seq($(outputLabelCol)))
      //.withColumn($(outputLabelCol), when($(outputLabelCol).isNull, 0).otherise($(outputLabelCol))
  }
}