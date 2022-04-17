package operator_companions.operators

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadFileOperator(spark: SparkSession, format: String, source: String, columns: Vector[String], header: Option[Boolean]) extends SparkOperator {
  val dataFrame: DataFrame = {
    val df = spark.read.format(format).option("header", header.getOrElse(false)).load(source)
    if (columns.nonEmpty) {
      df.select(columns.head, columns.tail: _*)
    } else {
      df
    }
  }

  override def execute(): DataFrame = {
    dataFrame
  }
}
