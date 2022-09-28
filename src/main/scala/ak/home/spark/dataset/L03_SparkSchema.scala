package ak.home.spark.dataset

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object L03_SparkSchema {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSchema")
      .master("local[2]")
      .getOrCreate()

    val simpleData = Seq(Row("James", "Smith", "36636", "M", 3000),
      Row("Michael", "Rose", "40288", "M", 4000),
      Row("Robert", "Williams", "42114", "M", 4000),
      Row("Maria", "Jones", "39192", "F", 4000),
      Row("Jen", "Brown", "", "F", -1)
    )

    // In Spark, we define it using StructType class which is a collection of StructField
    // that define the column name(String), column type (DataType), nullable column (Boolean)
    // and metadata (MetaData)
    val simpleSchema = StructType(Array(
      StructField("firstname", StringType, true),
      StructField("lastname", StringType, true),
      StructField("id", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", IntegerType, true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), simpleSchema)

    df.printSchema()

    df.createOrReplaceTempView("people")

    val resultDF = spark.sql("select * from people where gender = 'F'")
    resultDF.show()

    spark.stop()
  }

}
