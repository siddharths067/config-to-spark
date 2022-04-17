package operator_companions

import operator_companions.operators.{SparkOperator, UnionOperator}
import io.circe.Json
import org.apache.spark.sql.SparkSession

object UnionOperatorCompanion extends SparkOperatorCompanion[UnionOperator] {
  override def apply(sparkSession: SparkSession, inputs: Vector[SparkOperator], operatorConfig: Json): Option[UnionOperator] = {
    Some(new UnionOperator(sparkSession, inputs))
  }

  override def configString(): String = {
    "union_dataframe"
  }
}
