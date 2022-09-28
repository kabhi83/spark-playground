package ak.home.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{trim, col}

object L01_SparkDataSetBasics {

  def main(args: Array[String]): Unit = {

    //Initialize Spark Session
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkDataSetBasics")
      .getOrCreate()

    import spark.implicits._

    val studentsDS = spark.read
                        .option("header", "false")
                        .option("sep", ",")
                        .option("inferSchema", "true")
                        .csv("data/University_Students_Monthly_Expenses.csv")

    studentsDS.printSchema()

    // Select only Gender and Age column
    val studentsByAge = studentsDS.select("_c0", "_c1")
    studentsByAge.show(5)

    // Create a temp table for executing sql queries
    studentsByAge.createOrReplaceTempView("student")
    val sqlDF = spark.sql("SELECT count(*) FROM student where student._c0 like '%Female%'")
    sqlDF.show()


  }

}
