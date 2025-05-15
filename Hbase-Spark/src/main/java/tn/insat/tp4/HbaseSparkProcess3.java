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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class HbaseSparkProcess3 {

    // Coordinate class representing a station's coordinates
    public static class Coordinate implements Serializable {
        private static final long serialVersionUID = 1L;
        public String id;
        public double latitude;
        public double longitude;

        public Coordinate(String id, double latitude, double longitude) {
            this.id = id;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    // CoordinatesOutput class for organizing coordinates into a list
    public static class CoordinatesOutput implements Serializable {
        private static final long serialVersionUID = 2L;
        public List<Coordinate> stations;

        public CoordinatesOutput(List<Coordinate> stations) {
            this.stations = stations;
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

        // 3. Extract Coordinates (Latitude and Longitude)
        JavaRDD<Coordinate> coordinatesRDD = hBaseRDD.map(tuple -> {
            Result result = tuple._2;
            String id = Bytes.toString(result.getRow());

            double latitude = 0.0;
            double longitude = 0.0;

            // Read latitude as string and parse to double
            byte[] latBytes = result.getValue(Bytes.toBytes("location"), Bytes.toBytes("latitude"));
            if (latBytes != null && latBytes.length > 0) {
                try {
                    latitude = Double.parseDouble(Bytes.toString(latBytes)); // Convert string to double
                } catch (NumberFormatException e) {
                    System.err.println("Invalid latitude for ID " + id + ": " + Bytes.toString(latBytes));
                }
            }

            // Read longitude as string and parse to double
            byte[] lonBytes = result.getValue(Bytes.toBytes("location"), Bytes.toBytes("longitude"));
            if (lonBytes != null && lonBytes.length > 0) {
                try {
                    longitude = Double.parseDouble(Bytes.toString(lonBytes)); // Convert string to double
                } catch (NumberFormatException e) {
                    System.err.println("Invalid longitude for ID " + id + ": " + Bytes.toString(lonBytes));
                }
            }

            return new Coordinate(id, latitude, longitude);
        }).filter(coordinate -> coordinate.id != null);

        // 4. Collect the coordinates into a list
        List<Coordinate> coordinatesList = coordinatesRDD.collect();

        // 5. Create a JSON object to write the data
        CoordinatesOutput coordinatesOutput = new CoordinatesOutput(coordinatesList);

        // 6. Write the coordinates to a JSON file
        writeCoordinatesToJson(coordinatesOutput, "coordinates.json");

        // 7. Close SparkContext
        sc.close();
    }

    // Write coordinates to a JSON file
    private static void writeCoordinatesToJson(CoordinatesOutput coordinatesOutput, String outputFile) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.writeValue(new File(outputFile), coordinatesOutput);
            System.out.println("Coordinates data has been written to " + outputFile);
        } catch (IOException e) {
            System.err.println("Error writing coordinates to JSON: " + e.getMessage());
        }
    }
}