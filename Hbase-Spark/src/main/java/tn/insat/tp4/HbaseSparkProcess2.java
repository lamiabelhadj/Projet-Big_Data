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

public class HbaseSparkProcess2 {

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
        System.out.println("Total rows from HBase table: " + rawCount);

        // 3. Convert to Station objects with debug logs
        JavaRDD<Station> stationsRDD = hBaseRDD.map(tuple -> {
            Result result = tuple._2;
            String id = Bytes.toString(result.getRow());

            String fuelType = Bytes.toString(result.getValue(Bytes.toBytes("fuel"), Bytes.toBytes("fuel_type_code")));
            String state = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("state")));
            String access = Bytes.toString(result.getValue(Bytes.toBytes("access"), Bytes.toBytes("groups_with_access_code")));

            return new Station(id, fuelType, state, access);
        }).filter(station -> station.id != null);  // Ensure nulls are filtered

        long stationCount = stationsRDD.count();
        System.out.println("Total stations parsed: " + stationCount);

        // 4. State-wise aggregation
        JavaPairRDD<String, Tuple2<Long, Map<String, Long>>> stateAnalysisRDD = stationsRDD
                .mapToPair(station -> {
                    String stateKey = station.state != null ? station.state : "unknown";
                    Map<String, Long> fuelTypeCount = new HashMap<>();
                    fuelTypeCount.put(station.fuelType != null ? station.fuelType : "unknown", 1L);
                    return new Tuple2<>(stateKey, new Tuple2<>(1L, fuelTypeCount));
                })
                .reduceByKey((a, b) -> {
                    long count = a._1 + b._1;
                    Map<String, Long> mergedFuelTypeCount = new HashMap<>(a._2);
                    b._2.forEach((key, value) -> mergedFuelTypeCount.merge(key, value, Long::sum));
                    return new Tuple2<>(count, mergedFuelTypeCount);
                });

        // Convert to map for final result
        Map<String, Map<String, Object>> stateAnalysisMap = new HashMap<>();
        stateAnalysisRDD.collect().forEach(tuple -> {
            String state = tuple._1;
            long stationCountState = tuple._2._1;
            Map<String, Long> fuelTypeCountsState = tuple._2._2;
            Map<String, Object> stateInfo = new HashMap<>();
            stateInfo.put("station_count", stationCountState);
            stateInfo.put("fuel_type_counts", fuelTypeCountsState);
            stateAnalysisMap.put(state, stateInfo);
        });

        // Write to state_analysis.json
        ObjectMapper mapper = new ObjectMapper();
        File stateOutFile = new File("state_analysis.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(stateOutFile, stateAnalysisMap);

        System.out.println("State analysis JSON exported to: " + stateOutFile.getAbsolutePath());

        // 5. EV Analysis
        JavaRDD<Station> evStationsRDD = stationsRDD.filter(station -> "elec".equals(station.fuelType)); // Filter EV stations

        long totalEvStations = evStationsRDD.count();
        System.out.println("Total EV stations: " + totalEvStations);

        JavaPairRDD<String, Long> evAccessTypeCountsRDD = evStationsRDD
                .mapToPair(station -> new Tuple2<>(station.access != null ? station.access : "unknown", 1L))
                .reduceByKey(Long::sum);

        Map<String, Long> evAccessTypeCounts = evAccessTypeCountsRDD.collectAsMap();

        // 6. Build EV analysis JSON structure
        Map<String, Object> evAnalysisMap = new HashMap<>();
        evAnalysisMap.put("total_ev_stations", totalEvStations);
        evAnalysisMap.put("ev_access_type_counts", evAccessTypeCounts);

        // Write to ev_analysis.json
        File evOutFile = new File("ev_analysis.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(evOutFile, evAnalysisMap);

        System.out.println("EV analysis JSON exported to: " + evOutFile.getAbsolutePath());

        sc.close();
    }
}
