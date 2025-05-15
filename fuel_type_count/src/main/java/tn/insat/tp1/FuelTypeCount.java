package tn.insat.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FuelTypeCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fuel Type Count by State");

        job.setJarByClass(FuelTypeCount.class);
        job.setMapperClass(FuelMapper.class);
        job.setCombinerClass(FuelReducer.class);
        job.setReducerClass(FuelReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
