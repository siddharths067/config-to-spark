package operator_companions

import operator_companions.operators.{SaveFileOperator, SparkOperator}
import io.circe.Json
import org.apache.spark.sql.SparkSession

object SaveFileOperatorCompanion extends SparkOperatorCompanion[SaveFileOperator] {
  override def apply(sparkSession: SparkSession, inputs: Vector[SparkOperator], operatorConfig: Json): Option[SaveFileOperator] = {
    val options = (operatorConfig \\ "options").head
    val target = (options \\ "target").head.asString.get
    val format = (options \\ "format").head.asString.get
    val mode = (options \\ "mode").head.asString.get
    val saveFileOperator = new SaveFileOperator(sparkSession, mode, format, target, inputs)
    saveFileOperator.execute()
    Some(saveFileOperator)
  }

  override def configString(): String = "save_dataframe"
}
