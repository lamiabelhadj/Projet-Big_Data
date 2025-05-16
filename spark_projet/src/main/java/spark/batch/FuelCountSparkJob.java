package spark.batch;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class FuelCountSparkJob {
    //    public static void main(String[] args) {
//        if (args.length < 2) {
//            System.err.println("Usage: FuelCountSparkJob <input> <output>");
//            System.exit(1);
//        }
//
//        SparkConf conf = new SparkConf()
//                .setAppName("Fuel Station Count")
//                .setMaster("local[*]");  // <-- Add this line for local run
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> lines = sc.textFile(args[0]);
//
//        JavaPairRDD<String, Integer> counts = lines
//                .filter(line -> !line.startsWith("ID")) // Skip header
//                .mapToPair(line -> {
//                    String[] parts = line.split(",", -1);
//                    if (parts.length > 6) {
//                        String fuel = parts[1].trim().toLowerCase();
//                        String state = parts[6].trim().toLowerCase();
//                        return new Tuple2<>(fuel + "_" + state, 1);
//                    } else {
//                        return new Tuple2<>("invalid", 0);
//                    }
//                })
//                .filter(t -> !t._1.equals("invalid"))
//                .reduceByKey(Integer::sum);
//
//        counts.saveAsTextFile(args[1]);
//
//        sc.close();
//    }
//}
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: FuelCountSparkJob <input> <output>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Fuel Station Count")
//                .master("local[*]")  
                .getOrCreate();

        // Read CSV with proper parser
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("multiLine", false)
                .option("quote", "\"")
                .csv(args[0]);

        // Process: lowercase and group
        Dataset<Row> grouped = df
                .filter("`Fuel Type Code` IS NOT NULL AND State IS NOT NULL")
                .selectExpr("lower(`Fuel Type Code`) as fuel", "lower(State) as state")
                .groupBy("fuel", "state")
                .count();

        // Write output
        grouped
                .coalesce(2)
                .write()
                .option("header", "true")
                .csv(args[1]);


        spark.stop();
    }
}
