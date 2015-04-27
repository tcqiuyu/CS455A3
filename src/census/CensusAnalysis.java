package census;

import census.mapreduce.CensusMapper;
import census.mapreduce.CensusReducer;
import census.writableformat.CensusInfoFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 4/23/15.
 */
public class CensusAnalysis {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job census_Job = Job.getInstance(configuration, "census");

        census_Job.setJarByClass(CensusAnalysis.class);
        census_Job.setMapperClass(CensusMapper.class);
        census_Job.setReducerClass(CensusReducer.class);

        FileInputFormat.setInputPaths(census_Job, new Path(args[args.length - 2]));
        FileInputFormat.setInputDirRecursive(census_Job, true);
        census_Job.setInputFormatClass(TextInputFormat.class);

        census_Job.setMapOutputKeyClass(Text.class);
        census_Job.setMapOutputValueClass(CensusInfoFormat.class);

        FileSystem fs = FileSystem.get(configuration);
        Path outputPath = new Path(args[args.length - 1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }
        FileOutputFormat.setOutputPath(census_Job, new Path(args[args.length - 1]));

        MultipleOutputs.addNamedOutput(census_Job, "Q1", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(census_Job, "Q2", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(census_Job, "Q3", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(census_Job, "Q4", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(census_Job, "Q5", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(census_Job, "Q6", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(census_Job, "Q7", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(census_Job, "Q8", TextOutputFormat.class, Text.class, Text.class);

        census_Job.setOutputFormatClass(TextOutputFormat.class);
        census_Job.setOutputKeyClass(Text.class);
        census_Job.setOutputValueClass(Text.class);

        census_Job.waitForCompletion(true);
    }

}
