package org.terrier.spark.ltr

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors

object LTRUtils {
  
  
//  def convertRDD(input : RDD[QueryLabeledPoint]) : RDD[(Int, SparseVector[Short], Array[SplitInfo])] = {
//    
//    input.map{point => 
//      (point.label.toInt, new SparseVector[Short]( point.features ), new Array[SplitInfo](0))
//    }
//  }
  
  def loadLibSVMFile(sc: SparkContext, path: String): RDD[QueryLabeledPoint] =
    loadLibSVMFile(sc, path, -1)
    
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      numFeatures: Int): RDD[QueryLabeledPoint] =
    loadLibSVMFile(sc, path, numFeatures, sc.defaultMinPartitions)
    
  
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      numFeatures: Int,
      minPartitions: Int): RDD[QueryLabeledPoint] = {
    val parsed = parseLibSVMFile(sc, path, minPartitions)

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      computeNumFeatures(parsed)
    }

    parsed.map { case (label, qid, indices, values) =>
      new QueryLabeledPoint(qid, label, Vectors.sparse(d, indices, values))
    }
  }
  
  private def computeNumFeatures(rdd: RDD[(Double, String, Array[Int], Array[Double])]): Int = {
    rdd.map { case (label, qid, indices, values) =>
      indices.lastOption.getOrElse(0)
    }.reduce(math.max) + 1
  }

  private def parseLibSVMFile(
      sc: SparkContext,
      path: String,
      minPartitions: Int): RDD[(Double, String, Array[Int], Array[Double])] = {
    sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseLTRRecord)
  }
  
  def parseLTRRecord(line: String): (Double, String, Array[Int], Array[Double]) = {
    val items = line.replaceAll("#.*$","").split(' ')
    val label = items.head.toDouble
    val qid= items(1).split(':')(1)
    val (indices, values) = items.slice(2, items.length) .filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip

    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    while (i < indicesLength) {
      val current = indices(i)
      require(current > previous, s"indices should be one-based and in ascending order;"
        + " found current=$current, previous=$previous; line=\"$line\"")
      previous = current
      i += 1
    }
    (label, qid, indices.toArray, values.toArray)
  }
}