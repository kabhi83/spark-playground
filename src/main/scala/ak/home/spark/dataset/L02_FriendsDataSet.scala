package ak.home.spark.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, round}

object L02_FriendsDataSet {

  case class Friends(id: Int, name: String, age: Int, friends: Long)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
          .builder()
          .master("local[*]")
          .appName("FriendsDataSet")
          .getOrCreate()

    import spark.implicits._

    // Load each line of the source csv to Dataset
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/friends.csv")
      .as[Friends]

    //Select only the age and number of friends column
    val friendsByAgeDs = ds.select("age", "friends")

    // Group the Dataset by age
    friendsByAgeDs.groupBy("age").avg("friends").show(5)

    //Formatted and sorted
    friendsByAgeDs.groupBy("age").agg(round(avg("friends"), 2)).sort()

    //Formatted, sorted and custom column name
    friendsByAgeDs.groupBy("age").agg(round(avg("friends"), 2))
      .alias("avg_friends_by_age").sort()
  }

}
