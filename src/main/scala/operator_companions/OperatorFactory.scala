package operator_companions

import operator_companions.operators.SparkOperator
import io.circe.Json
import org.apache.spark.sql.SparkSession

object OperatorFactory {
  def apply(
             sparkSession: SparkSession, operatorConfig: Json,
             keyOperatorMap: Map[String, Option[SparkOperator]],
             operatorCompanions: Seq[SparkOperatorCompanion[_ <: SparkOperator]]
           ): Option[SparkOperator] = {
    val operatorType = (operatorConfig \\ "type").headOption
      .flatMap(_.asString)
      .getOrElse("")
    val inputList = (operatorConfig \\ "inputs").headOption
      .flatMap(e => e.asArray)
      .getOrElse(Vector[Json]())
      .map(_.asString)
      .filter(_.nonEmpty)
      .map(_.get)
    val inputOperators = inputList.foldLeft(Vector[Option[SparkOperator]]())((acc, opKey) => acc :+ keyOperatorMap.getOrElse(opKey, None))
    val initialisedInputOperators = inputOperators.filter(_.nonEmpty).map(_.get)
    val operatorKeyCompanionMap: Map[String, SparkOperatorCompanion[_ <: SparkOperator]] = operatorCompanions.map(companion => (companion.configString(), companion)).toMap
    operatorKeyCompanionMap.get(operatorType) match {
      case Some(companion) => companion(sparkSession, initialisedInputOperators, operatorConfig)
      case _ =>
        println("Non-existent operator please check the type of operator specified")
        None
    }
  }
}
