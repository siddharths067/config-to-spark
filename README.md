# Config To Spark
A simple repository to demonstrate simple patterns like composite, factories and strategies.
Used to convert a YAML configuration into a spark job.

# Example configuration

```yaml
---
job_name: example_job
job_description: An example job to demonstrate config to spark job conversion
operators:
  read_one:
    type: read_file
    options:
      source: "/tmp/example.csv"
      format: "csv"
      header: true
      columns:
        - A
        - B
  read_two:
    type: read_file
    options:
      source: "/tmp/example.csv"
      format: "csv"
      header: true
      columns:
        - A
        - B
  union_operator:
    inputs:
      - read_one
      - read_two
    type: union_dataframe

  save_operator:
    type: save_dataframe
    inputs:
      - union_operator
    options:
      target: "/tmp/example_output.csv"
      format: "csv"
      mode: "overwrite"
...
```

Each job document contains a job name and description.

Each operator entry has 
```yaml
    operator_name:
      type: operator_key
      input:
        - previous_operator
        - previous_operator_2
      options:
        - custom yaml config that is used to initialise your operator
```

To implement your custom operator implement the SparkOperator and SparkOperatorCompanion trait

```scala
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
```

Then pass the companions to runConfigToSpark function
```yaml
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
```