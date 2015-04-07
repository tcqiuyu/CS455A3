package gigasort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 4/6/15.
 * Main Class
 * Sort Giga-level long numbers
 */

public class Gigasort {

    public static final int REDUCER_NUM = 16;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setNumReduceTasks(REDUCER_NUM);
        job.setJarByClass(Gigasort.class);
        job.setMapperClass(SortMapper.class);
//        job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);
        job.setPartitionerClass(SortPartitioner.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
