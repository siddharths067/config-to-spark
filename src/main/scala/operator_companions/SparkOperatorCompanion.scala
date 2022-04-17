package operator_companions

import operator_companions.operators.SparkOperator
import io.circe.Json
import org.apache.spark.sql.SparkSession

trait SparkOperatorCompanion[T <: SparkOperator] {
  def apply(sparkSession: SparkSession, inputs: Vector[SparkOperator], operatorConfig: Json): Option[T]

  def configString(): String
}
