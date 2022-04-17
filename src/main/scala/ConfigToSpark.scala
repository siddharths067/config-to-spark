import ConfigRunner.runConfigToSpark
import operator_companions.operators.SparkOperator
import operator_companions.{ReadFileOperatorCompanion, SaveFileOperatorCompanion, SparkOperatorCompanion, UnionOperatorCompanion}

import scala.collection.immutable


object ConfigToSpark extends App {
  private val sparkOperatorCompanions: immutable.Seq[SparkOperatorCompanion[_ <: SparkOperator]] =
    Vector[SparkOperatorCompanion[_ <: SparkOperator]](ReadFileOperatorCompanion,
      UnionOperatorCompanion,
      SaveFileOperatorCompanion)
  args.foreach(configPath => {
    println("Running Config " + configPath)
    runConfigToSpark(configPath, sparkOperatorCompanions)
  })
}