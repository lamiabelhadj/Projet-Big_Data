package tn.insat.tp4;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class HbaseSparkProcess1 {

    public static class Station {
        public String id;
        public String fuelType;
        public String state;
        public String access;

        public Station(String id, String fuelType, String state, String access) {
            this.id = id;
            this.fuelType = fuelType;
            this.state = state;
            this.access = access;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. HBase + Spark Configuration
        Configuration config = HBaseConfiguration.create();
        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        config.set(TableInputFormat.INPUT_TABLE, "fuel_stations");

        // 2. Load HBase data
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                sc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        long rawCount = hBaseRDD.count();
        System.out.println(" Total rows from HBase table: " + rawCount);

        // 3. Convert to Station objects with debug logs
        JavaRDD<Station> stationsRDD = hBaseRDD.map(tuple -> {
            Result result = tuple._2;
            String id = Bytes.toString(result.getRow());

            // Debug info
            System.out.println("Processing row: " + id);

            String fuelType = Bytes.toString(result.getValue(Bytes.toBytes("fuel"), Bytes.toBytes("fuel_type_code")));
            String state = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("state")));
            String access = Bytes.toString(result.getValue(Bytes.toBytes("access"), Bytes.toBytes("groups_with_access_code")));

            return new Station(id, fuelType, state, access);
        }).filter(station -> station.id != null);  // Ensure nulls are filtered

        long stationCount = stationsRDD.count();
        System.out.println(" Total stations parsed: " + stationCount);

        // 4. Aggregations
        Map<String, Long> fuelTypeCounts = stationsRDD
                .mapToPair(s -> new Tuple2<>(s.fuelType != null ? s.fuelType : "unknown", 1L))
                .reduceByKey(Long::sum)
                .collectAsMap();

        Map<String, Long> stateCounts = stationsRDD
                .mapToPair(s -> new Tuple2<>(s.state != null ? s.state : "unknown", 1L))
                .reduceByKey(Long::sum)
                .collectAsMap();

        Map<String, Long> accessTypeCounts = stationsRDD
                .mapToPair(s -> new Tuple2<>(s.access != null ? s.access : "unknown", 1L))
                .reduceByKey(Long::sum)
                .collectAsMap();

        // 5. Build final JSON structure
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("total_stations", stationCount);
        resultMap.put("fuel_type_counts", fuelTypeCounts);
        resultMap.put("state_counts", stateCounts);
        resultMap.put("access_type_counts", accessTypeCounts);

        // 6. Write to JSON file
        ObjectMapper mapper = new ObjectMapper();
        File outFile = new File("fuel_summary.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(outFile, resultMap);

        System.out.println(" JSON exported to: " + outFile.getAbsolutePath());

        sc.close();
    }
}
