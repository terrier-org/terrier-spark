package org.apache.spark.ml.source.libsvm

import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.TextBasedFileFormat
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import java.io.IOException
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
//import org.apache.spark.ml.source.libsvm.LibSVMOutputWriter
import org.apache.spark.sql.execution.datasources.HadoopFileLinesReader
import org.apache.spark.sql.types.StructField
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types.MetadataBuilder
import ch.qos.logback.core.filter.Filter
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.DataTypes

import org.apache.spark.TaskContext
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, VectorUDT}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
//import org.apache.spark.ml.source.libsvm.LibSVMOutputWriter
import org.terrier.spark.ltr.QueryLabeledPoint
import org.terrier.spark.ltr.LTRUtils
import org.terrier.spark.ltr.QueryLabeledPoint

class LTRSource extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider with DataSourceRegister {
  
}

private class LTRFileFormat extends TextBasedFileFormat with DataSourceRegister {

  val NUM_FEATURES = "numFeatures"
  
  override def shortName(): String = "ltr"

  override def toString: String = "LTR"

  private def verifySchema(dataSchema: StructType): Unit = {
    if (
      dataSchema.size != 3 ||
        !dataSchema(0).dataType.sameType(DataTypes.DoubleType) ||
        !dataSchema(1).dataType.sameType(DataTypes.StringType) ||
        !dataSchema(2).dataType.sameType(new VectorUDT()) ||
        !(dataSchema(2).metadata.getLong(NUM_FEATURES).toInt > 0)
    ) {
      throw new IOException(s"Illegal schema for ltr data, schema=$dataSchema")
    }
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    val libSVMOptions = new LibSVMOptions(options)
    val numFeatures: Int = libSVMOptions.numFeatures.getOrElse {
      // Infers number of features if the user doesn't specify (a valid) one.
      val dataFiles = files.filterNot(_.getPath.getName startsWith "_")
      val path = if (dataFiles.length == 1) {
        dataFiles.head.getPath.toUri.toString
      } else if (dataFiles.isEmpty) {
        throw new IOException("No input path specified for libsvm data")
      } else {
        throw new IOException("Multiple input paths are not supported for libsvm data.")
      }

      val sc = sparkSession.sparkContext
      val parsed = MLUtils.parseLibSVMFile(sc, path, sc.defaultParallelism)
      MLUtils.computeNumFeatures(parsed)
    }

    val featuresMetadata = new MetadataBuilder()
      .putLong(LibSVMOptions.NUM_FEATURES, numFeatures)
      .build()

    Some(
      StructType(
        StructField("label", DoubleType, nullable = false) ::
        StructField("features", new VectorUDT(), nullable = false, featuresMetadata) :: Nil))
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new LibSVMOutputWriter(path, dataSchema, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".libsvm" + CodecStreams.getCompressionExtension(context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    verifySchema(dataSchema)
    val numFeatures = dataSchema("features").metadata.getLong(LibSVMOptions.NUM_FEATURES).toInt
    assert(numFeatures > 0)

    val libSVMOptions = new LibSVMOptions(options)
    val isSparse = libSVMOptions.isSparse

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val linesReader = new HadoopFileLinesReader(file, broadcastedHadoopConf.value.value)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))

      val points = linesReader
          .map(_.toString.trim)
          .filterNot(line => line.isEmpty || line.startsWith("#"))
          .map { line =>
            val (label, qid, indices, values) = LTRUtils.parseLTRRecord(line)
            new QueryLabeledPoint(qid, label, Vectors.sparse(numFeatures, indices, values))
          }

      val converter = RowEncoder(dataSchema)
      val fullOutput = dataSchema.map { f =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      }
      val requiredOutput = fullOutput.filter { a =>
        requiredSchema.fieldNames.contains(a.name)
      }

      val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)

      points.map { pt =>
        val features = if (isSparse) pt.features.toSparse else pt.features.toDense
        requiredColumns(converter.toRow(Row(pt.label, features)))
      }
    }
  }
}

