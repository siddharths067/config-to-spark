package operator_companions

import operator_companions.operators.{ReadFileOperator, SparkOperator}
import io.circe.Json
import org.apache.spark.sql.SparkSession

object ReadFileOperatorCompanion extends SparkOperatorCompanion[ReadFileOperator] {
  override def apply(sparkSession: SparkSession, inputs: Vector[SparkOperator], operatorConfig: Json): Option[ReadFileOperator] = {
    val options = (operatorConfig \\ "options").head
    val source = (options \\ "source").head.asString.get
    val format = (options \\ "format").head.asString.get
    val header = (options \\ "header").headOption.flatMap(_.asBoolean)
    val columns: Vector[String] = (options \\ "columns").headOption
      .flatMap(_.asArray)
      .getOrElse(Vector())
      .map(_.asString)
      .filter(_.isDefined)
      .map(_.get)
    Some(new ReadFileOperator(sparkSession, format, source, columns, header))
  }

  override def configString(): String = "read_file"
}
