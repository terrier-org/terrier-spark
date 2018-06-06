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
import org.apache.hadoop.mapred.IndexCache
import org.terrier.querying.LocalManager
import org.terrier.querying.Request

object TerrierQueryMapClient {
  val managerCache = scala.collection.mutable.Map[Properties,Manager]();
  val indexCache = scala.collection.mutable.Map[(String,String),Index]();
  
}

/** This is the overall client code for accessing a terrier instance from Spark */
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
    val indexLocation = (ApplicationSetup.TERRIER_INDEX_PATH,ApplicationSetup.TERRIER_INDEX_PREFIX)
    
    val index : Index = 
      if (TerrierQueryMapClient.indexCache.contains(indexLocation))
           TerrierQueryMapClient.indexCache.get(indexLocation).get
      else
      {
        System.err.println(Thread.currentThread().getContextClassLoader.toString());
        System.err.println("Loading index, indexCache had " + TerrierQueryMapClient.indexCache.size + " items" )
        val tmp = Index.createIndex()
        TerrierQueryMapClient.indexCache.put(indexLocation, tmp)
        tmp
      }
    if (index == null)
      throw new IllegalArgumentException("Index not found: " + Index.getLastIndexLoadError)
    val m : Manager = new LocalManager(index)
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
    //c is a special case, as its defined by a control, not a property.
    if (props.contains("c"))
    {
        srq.setControl("c", props.get("c").get)
        srq.setControl("c_set", "true")
    }
    srq.addMatchingModel(
        ApplicationSetup.getProperty("trec.matching", matching), 
        ApplicationSetup.getProperty("trec.model", wmodel)) 
    manager.runSearchRequest(srq)
    val rtr = srq.asInstanceOf[Request].getResultSet
    (qid, rtr)
  }
}