import io.circe.Json
import io.circe.yaml.parser
import operator_companions.operators.SparkOperator
import operator_companions.{OperatorFactory, SparkOperatorCompanion}
import org.apache.spark.sql.SparkSession

import java.io.{FileInputStream, InputStreamReader}
import scala.collection.immutable

object ConfigRunner {

  def runConfigToSpark(configPath: String, sparkOperatorCompanions: immutable.Seq[SparkOperatorCompanion[_ <: SparkOperator]]): Unit = {

    println("Welcome to Config to Spark")

    def getValidJobConfigurationsFromFile(configPath: String): Stream[Json] = {
      val fileInputStream = new FileInputStream(configPath)
      parser.parseDocuments(new InputStreamReader(fileInputStream))
        .map {
          case Right(value) => Some(value)
          case Left(error) =>
            println("There was an error parsing document")
            println(error)
            None
        }.filter(e => e.isDefined).map(e => e.get)
    }

    val validJobConfigurations: Stream[Json] = getValidJobConfigurationsFromFile(configPath)

    validJobConfigurations.foreach(jobConfigJson => {
      val jobName = (jobConfigJson \\ "job_name").headOption
      val jobDescription = (jobConfigJson \\ "job_description").headOption
      val operatorConfig = (jobConfigJson \\ "operators").headOption
      if (jobName.nonEmpty && jobDescription.nonEmpty && operatorConfig.nonEmpty) {
        println("Creating new Spark Session for Job : " + jobName.get.toString())
        println("Job Description : " + jobDescription.get.toString())
        val sparkSession = SparkSession.builder().master("local[*]").appName(jobName.get.toString()).getOrCreate()
        val operatorConfigObject = operatorConfig.get.asObject.get
        val keyOperatorMap = operatorConfigObject.keys.foldLeft(Map[String, Option[SparkOperator]]())((keyOperatorMap, key) => {
          val operatorJson = operatorConfigObject.apply(key)
          keyOperatorMap + (operatorJson match {
            case Some(operatorSpecificConfig) => (key, OperatorFactory(sparkSession, operatorSpecificConfig, keyOperatorMap, sparkOperatorCompanions))
            case None => (key, None)
          })
        })
      }
    })
  }

}
