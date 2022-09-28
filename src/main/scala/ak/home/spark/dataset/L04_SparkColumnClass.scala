package ak.home.spark.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
* The Spark Column class defines a variety of column methods
* that are vital for manipulating DataFrames.
**/

object L04_SparkColumnClass {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkColumnClass")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val df = Seq(
      ("Thor", "New York"),
      ("Aquaman", "Atlantis"),
      ("Wolverine", "New York"),
      ("Chota Bheem", "New Delhi")
    ).toDF("superhero", "city")

    // A Column object is instantiated with the $"city" statement
    df.withColumn("City_Starts_With_New", $"city".startsWith("New")).show()

    //A Column object is created with the df("city") statement
    df.withColumn("City_Starts_With_New", df("city")).show()

    //A Column object is created with the col("city") statement - preferred
    df.withColumn("City_Starts_With_New", col("city").startsWith("New")).show()

    val df1 = Seq(
      (1, "Rishabh", 18, "M"),
      (2, "Nikita", 14, "F"),
      (3, "Rahul", 17, "M"),
      (4, "Mridul", 19, "M"),
      (5, "Mallika", 17, "F"),
      (6, "Rakesh", 18, "M")
    ).toDF("id", "name", "age", "gender")

    df1.withColumn("Adult", col("age").geq(18)).show()


    spark.stop()
  }

}
