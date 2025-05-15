import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EVAnalysisStream {
  def main(args: Array[String]): Unit = {
    // Start Spark session in local mode
    val spark = SparkSession.builder()
      .appName("EV Stream Analysis")
      .master("local[*]")
      .getOrCreate()

    // Define the schema explicitly for streaming CSV input
    val schema = StructType(Array(
      StructField("ID", IntegerType),
      StructField("Fuel Type Code", StringType),
      StructField("Station Name", StringType),
      StructField("Street Address", StringType),
      StructField("Intersection Directions", StringType),
      StructField("City", StringType),
      StructField("State", StringType),
      StructField("ZIP", StringType),
      StructField("Station Phone", StringType),
      StructField("Status Code", StringType),
      StructField("Groups With Access Code", StringType),
      StructField("Access Days Time", StringType),
      StructField("Cards Accepted", StringType),
      StructField("EV Level2 EVSE Num", IntegerType),
      StructField("EV Network", StringType),
      StructField("EV Network Web", StringType),
      StructField("Geocode Status", StringType),
      StructField("Latitude", DoubleType),
      StructField("Longitude", DoubleType),
      StructField("Date Last Confirmed", StringType),
      StructField("Updated At", StringType),
      StructField("Owner Type Code", StringType),
      StructField("Open Date", StringType),
      StructField("Location", StringType)
    ))

    // Read CSV files as a streaming source from HDFS
    val streamingDF = spark.readStream.option("header", "true").schema(schema)
      .csv("hdfs://hadoop-master:9000/stream_input")

    // Perform live aggregation: count stations by state
    val stateCounts = streamingDF.groupBy("State").count()

    // Write output to console in 'complete' mode
    val query = stateCounts.writeStream.outputMode("complete")
      .format("console").option("truncate", false).start()

    // Keep the streaming query running
    query.awaitTermination()
  }
}