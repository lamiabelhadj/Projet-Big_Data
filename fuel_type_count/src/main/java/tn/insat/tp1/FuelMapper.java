package tn.insat.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.io.StringReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class FuelMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (line.startsWith("ID,Fuel Type Code")) return;

        try (CSVParser parser = new CSVParser(new StringReader(line), CSVFormat.DEFAULT)) {
            for (CSVRecord record : parser) {
                if (record.size() > 6) {
                    String fuel = record.get(1).trim().toLowerCase();
                    String state = record.get(6).trim().toLowerCase();

                    if (!fuel.isEmpty() && !state.isEmpty()) {
                        outputKey.set(fuel + "_" + state);
                        context.write(outputKey, one);
                    }
                }
            }
        } catch (Exception e) {
            // Optionally log or skip malformed lines
        }
    }
}
