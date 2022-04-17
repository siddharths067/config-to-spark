package operator_companions.operators

import org.apache.spark.sql.DataFrame

trait SparkOperator {
  def execute(): DataFrame
}
