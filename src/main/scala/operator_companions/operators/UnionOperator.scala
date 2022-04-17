package operator_companions.operators

import org.apache.spark.sql.{DataFrame, SparkSession}

class UnionOperator(spark: SparkSession, inputs: Vector[SparkOperator]) extends SparkOperator {
  val dataFrame: DataFrame = inputs.map(_.execute()).reduce((a, b) => a.unionAll(b))

  override def execute(): DataFrame = {
    dataFrame
  }
}
