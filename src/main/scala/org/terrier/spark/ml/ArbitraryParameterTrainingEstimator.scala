package org.terrier.spark.ml

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.Estimator

import com.github.bruneli.scalaopt.core._
import derivativefree.NelderMead._
import org.apache.spark.ml.param.Param
import com.github.bruneli.scalaopt.core.derivativefree.NelderMeadConfig

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
  
//TODO expand this to handle multiple parameters
class ArbitraryParameterTrainingEstimator(override val uid: String)
  extends Estimator[PropertyModel]
  with QueryingPipelineStage with NeedQrels
{
  final val paramName = new Param[String](this, "paramName", "The names of the parameter to opt")
  final val paramValueInitial = new Param[Double](this, "paramValue", "The initial value of the parameter to opt")
  final val paramValueMax = new Param[Double](this, "paramValueMax", "The max value of the parameter to opt")
  final val paramValueMin = new Param[Double](this, "paramValueMin", "The max value of the parameter to opt")
  final val measureTol = new Param[Double](this, "measureTol", "The error tolerance, default 1e-5")
  final val optMaxIter = new Param[Int](this, "optMaxIter", "The maximum number of iterations, default 200")
  setDefault(measureTol, 1e-5)
  setDefault(optMaxIter, 200)
  
  def this() = this(Identifiable.randomUID("ArbitraryParameterTrainingEstimator"))
  
  override def copy(extra: ParamMap): ArbitraryParameterTrainingEstimator = {
    defaultCopy(extra)
  }
  
  override def fit(dataset: Dataset[_]): PropertyModel = {
    import dataset.sparkSession.implicits._
    val addQrelStage = new QrelTransformer()
    addQrelStage.set(addQrelStage.qrelsFile, get(this.qrelsFile).get)
    val ndcgStage = new NDCGEvalutor(20)
    val config = new NelderMeadConfig(tol = get(measureTol).get, maxIter = get(optMaxIter).get)
    
    val objf = new ObjectiveFunction
    {
      def apply(x: Variables): Double = {
        val value = x(0)
        
        if (get(paramValueMax).isDefined && value > get(paramValueMax).get)
        {
            System.err.println("OOB "+ get(paramName).get + "=" + value)
            0 + (value - get(paramValueMax).get)
        }
        else if (get(paramValueMin).isDefined && value < get(paramValueMin).get)
        {
            System.err.println("OOB "+ get(paramName).get + "=" + value)
            0 + (get(paramValueMin).get - value)
        }
        else
        {
          System.err.println("Evaluating "+ get(paramName).get + "=" + value)
          setTerrierProperties( get(localTerrierProperties).get + (get(paramName).get -> x(0).toString))
          val ndcg = -1 * ndcgStage.evaluate( addQrelStage.transform( transform(dataset) ) ) 
          System.err.println(get(paramName).get + "=" + value + " => " + ndcg)
          ndcg
        }
      }
    }
    val tuned = minimize(objf, Vector(get(paramValueInitial).get))(config)
    val bestParameter = tuned.get(0)
    System.err.println("Best parameter " + bestParameter) 
    val model = new PropertyModel("blabla", Map((get(paramName).get, bestParameter)))
    model.setTerrierProperties(getTerrierProperties())
    model
  }
  
}