package org.terrier.spark.ml

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

import com.github.bruneli.scalaopt.core.MaxIterException
import com.github.bruneli.scalaopt.core.ObjectiveFunction
import com.github.bruneli.scalaopt.core.Variables
import com.github.bruneli.scalaopt.core.derivativefree.NelderMead.minimize
import com.github.bruneli.scalaopt.core.derivativefree.NelderMeadConfig
import org.terrier.structures.IndexFactory


class PropertyModel(
  override val uid: String,
  val propertySettings: Map[String,Double])
  extends Model[PropertyModel]
  with QueryingPipelineStage
{
  override def copy(extra: ParamMap): PropertyModel = {
    defaultCopy(extra)
  }
  
  override def getTerrierProperties() = {
    super.getTerrierProperties() ++ propertySettings.map{ case (k : String, v : Double) => (k,v.toString)}
  }
  
}

/** An estimator that can be placed in a pipeline to vary 
 *  properties, in order to optimise them
 */
class ArbitraryParameterTrainingEstimator(override val uid: String)
  extends Estimator[PropertyModel]
  with QueryingPipelineStage with NeedQrels
{
  final val measure = new Param[Measure.Value](this, "measure", "The measure to opt")
  final val paramName = new Param[Seq[String]](this, "paramName", "The names of the parameter[s] to opt")
  final val paramValueInitial = new Param[Seq[Double]](this, "paramValue", "The initial value of the parameter[s] to opt")
  final val paramValueMax = new Param[Double](this, "paramValueMax", "The max value of the parameter to opt")
  final val paramValueMin = new Param[Double](this, "paramValueMin", "The max value of the parameter to opt")
  final val measureTol = new Param[Double](this, "measureTol", "The error tolerance, default 1e-5")
  final val optMaxIter = new Param[Int](this, "optMaxIter", "The maximum number of iterations, default 200")
  final val measureCutoff = new Param[Int](this, "measureCutoff", "The maximum number of results to evaluate, defaults to 20")
  setDefault(measureTol, 1e-5)
  setDefault(optMaxIter, 200)
  setDefault(measureCutoff, 20)
  
  def this() = this(Identifiable.randomUID("ArbitraryParameterTrainingEstimator"))
  
  override def copy(extra: ParamMap): ArbitraryParameterTrainingEstimator = {
    defaultCopy(extra)
  }
  
  def cache(parentfn : ObjectiveFunction) : ObjectiveFunction = {
    new ObjectiveFunction
    {
      val cache = scala.collection.mutable.Map[Variables,Double]()
      def apply(x: Variables): Double = {
        cache.contains(x) match {
          case true => cache.get(x).get
          case false => {
            val rtr = parentfn.apply(x)
            cache.put(x, rtr)
            rtr
          }
        }
      }
    }
  }
  
  override def fit(dataset: Dataset[_]): PropertyModel = {
    
    require(IndexFactory.isLocal($(indexRef)), 
        "indexref must be for a local index - e.g. remote indices not yet supported")
    require(get(paramName).get.length ==  get(paramValueInitial).get.length, 
        "Must have correct number of parameters to optimise")
    require(get(this.qrelsFile).isDefined, "qrels file must be set")
    require(get(this.measure).isDefined, "measure must be set")
    require(get(this.measureCutoff).isDefined, "measureCutoff must be set")
    
    val addQrelStage = new QrelTransformer()
    addQrelStage.set(addQrelStage.qrelsFile, get(this.qrelsFile).get)
    val evalStage = new RankingEvaluator(get(this.measure).get, get(this.measureCutoff).get)
    val config = new NelderMeadConfig(tol = get(measureTol).get, maxIter = get(optMaxIter).get)
    
    val objf = new ObjectiveFunction
    {
      
      def apply(x: Variables): Double = {
        //val value = x(0)
        
        val matched = x.zipWithIndex.map{ case (value, i) =>
          if (get(paramValueMax).isDefined && value > get(paramValueMax).get)
          {
             val rtr = 0 + (value - get(paramValueMax).get)
             System.err.println("OOB "+ get(paramName).get(i) + "=" + value + " => " + rtr)
             rtr
              
          }
          else if (get(paramValueMin).isDefined && value < get(paramValueMin).get)
          {
             val rtr = 0 + (get(paramValueMin).get - value)
             System.err.println("OOB "+ get(paramName).get(i) + "=" + value  + " => " + rtr)
             rtr
          }
          else {
            Double.MinValue
          }
        }
        //matched will contain Double.MinValue if no parameters are OOB
        if (matched.max > Double.MinValue)
          matched.max
        else
        {
          System.err.println("Evaluating "+ get(paramName).get + "=" + x)
          setTerrierProperties( get(localTerrierProperties).get ++ get(paramName).get.zipWithIndex.map{ case (name,index) => (name, x(index).toString) } )
          val ndcg = -1 * evalStage.evaluate( addQrelStage.transform( transform(dataset) ) ) 
          System.err.println(get(paramName).get + "=" + x + " => " + ndcg)
          ndcg
        }
      }
    }
    val finalFn = cache(objf)
    val paramsWithValues = try{
      val tuned = minimize(finalFn, get(paramValueInitial).get.toVector)(config)
      val bestParameter = tuned.get(0)
      System.err.println("Best parameters " + tuned.get  + " with eval " + finalFn.apply(tuned.get)) 
      val localParamsWithValues = get(paramName).get.zipWithIndex.map{ case (name,index) => (name, tuned.get(index)) }
      localParamsWithValues
    } catch {
      case e: MaxIterException => {
        get(paramName).get.zipWithIndex.map{ case (name,index) => (name, get(paramValueInitial).get(index)) }
      }
    }
    val model = new PropertyModel("blabla", Map()++paramsWithValues )
    model.setTerrierProperties(getTerrierProperties())
    model
  }
  
}
