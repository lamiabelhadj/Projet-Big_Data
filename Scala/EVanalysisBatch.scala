import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EVAnalysisBatch {
  def main(args: Array[String]): Unit = {
    // Start Spark session in local mode
    val spark = SparkSession.builder()
      .appName("EV Batch Analysis")
      .master("local[*]")
      .getOrCreate()

    // Load CSV data from HDFS with schema inference
    val df = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("hdfs://hadoop-master:9000/user/root/cleaned_alternative_fuel_locations.csv")

    // 1. Compute average number of EV Level2 ports per state
    df.groupBy("State")
      .agg(avg("EV Level2 EVSE Num").alias("avg_ports"))
      .orderBy(desc("avg_ports"))
      .show()

    // 2. Compute public vs. private station ratio per state
    val accessCodePivot = df.groupBy("State", "Groups With Access Code").count()
      .groupBy("State").pivot("Groups With Access Code").sum("count").na.fill(0)

    accessCodePivot.withColumn("public_private_ratio", col("Public") / (col("Private") + lit(1)))
      .select("State", "Public", "Private", "public_private_ratio")
      .orderBy(desc("public_private_ratio"))
      .show()

    // 3. Count number of stations per city
    df.groupBy("City")
      .count()
      .withColumnRenamed("count", "station_count")
      .orderBy(desc("station_count"))
      .show()

    // 4. Distribution of fuel types in selected cities
    df.filter(col("City").isin("Chicago", "Springfield"))
      .groupBy("City", col("Fuel Type Code"))
      .count()
      .orderBy(col("City"), desc("count"))
      .show()

    // 5. Count station openings by year from Open Date column
    val withYear = df.withColumn("year", year(to_date(col("Open Date"), "MM/dd/yyyy")))
    withYear.groupBy("year").count().orderBy("year").show()

    // Stop Spark session
    spark.stop()
  }
}