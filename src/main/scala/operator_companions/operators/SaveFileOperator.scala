package operator_companions.operators

import org.apache.spark.sql.{DataFrame, SparkSession}

class SaveFileOperator(spark: SparkSession, mode: String, format: String, target: String, inputs: Vector[SparkOperator]) extends SparkOperator {
  override def execute(): DataFrame = {
    val unionOfInputs = new UnionOperator(spark, inputs).execute()
    unionOfInputs.write.format(format).mode(mode).option("header", "true").save(target)
    unionOfInputs
  }
}
