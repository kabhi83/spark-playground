package ak.home.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object L01_SparkBasics {

  def parseline(line: String): String = {
    val fields = line.split(",")
    val gender = fields(0).trim
    gender
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //Utilize all CPU cores
    val spark = new SparkContext("local[*]", "SparkBasics")

    val rddFromFile = spark.textFile("data/University_Students_Monthly_Expenses.csv")
    val header = rddFromFile.first()
    // extract header
    val filteredRdd = rddFromFile.filter(row => row != header)

    val numOfLines = filteredRdd.count()

    println("No. of lines in the dataset is " + numOfLines)

    //Count Male student and Female student
    val parsedLines = filteredRdd.map(parseline)

    val maleCount = parsedLines.filter(line => line == "Male").count()
    val femaleCount = parsedLines.filter(line => line == "Female").count()

    println("No. of male students are " + maleCount + " and the number of female students are " + femaleCount)

    spark.stop()
  }
}
