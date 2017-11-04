import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model


trait WSDWeightingCAParams extends Params {
  final val MAX_STEPS = new Param[Double](this, "MAX_STEPS", "MAX_STEPS")
}

class WSDWeightingCAModel(
  override val uid: String, featureWeights: Map[Int,Double]) extends Model[WSDWeightingCAModel]
with WSDWeightingCAParams 
{
   override def copy(extra: ParamMap): WSDWeightingCAModel = {
    defaultCopy(extra)
  }
  
  
}

class WSDWeightingCA(override val uid: String) extends Estimator[WSDWeightingCAModel] 
{
  def this() = this(Identifiable.randomUID("WSDWeightingCA"))
  
  override def fit(dataset: Dataset[_]): WSDWeightingCAModel = {
    import dataset.sparkSession.implicits._
    
  }
}