package org.terrier.spark

import java.util.Properties

import org.apache.spark.api.java.function.MapFunction
import org.terrier.matching.ResultSet
import org.terrier.querying.Manager
import org.terrier.structures.Index
import org.terrier.structures.IndexOnDisk
import org.terrier.utility.ApplicationSetup
import java.io.IOException
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.Serializable

object TerrierQueryMapClient {
  val managerCache = scala.collection.mutable.Map[Properties,Manager]();
}

class TerrierQueryMapClient(props : Map[String,String]) extends ( ((String,String)) => (String,ResultSet)) with Serializable {
  
  var matching = "org.terrier.matching.daat.Full"
  var wmodel = "InL2"
  
  def getManager() = {
    val jprops : Properties  = new Properties();
    for ((k,v) <- props) jprops.setProperty(k, v)
    val manager = TerrierQueryMapClient.managerCache.getOrElse(jprops, newManager(jprops))
    TerrierQueryMapClient.managerCache.put(jprops, manager)
    manager
  }
  
  def newManager(props : Properties) = 
  {
    ApplicationSetup.clearAllProperties();
    ApplicationSetup.bootstrapInitialisation(props)
    val index : Index = Index.createIndex()
    if (index == null)
      throw new IllegalArgumentException("Index not found: " + Index.getLastIndexLoadError)
    val m : Manager = new Manager(index)
    m
  }
 
  override protected def	finalize() : Unit = {
    
  }
  
  def apply(input : (String, String)):(String,ResultSet) = {
    val m = getManager()
    runQuery(m,(input._1,input._2))
  }
  
  def runQuery(manager : Manager, input: (String,String)): (String,ResultSet) = {
    val qid = input._1
    val query = input._2
    val srq = manager.newSearchRequest(qid, query)
    srq.addMatchingModel(
        ApplicationSetup.getProperty("trec.matching", matching), 
        ApplicationSetup.getProperty("trec.model", wmodel)) 
    manager.runSearchRequest(srq)
    val rtr = srq.getResultSet
    (qid, rtr)
  }
}