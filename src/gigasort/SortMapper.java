package gigasort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu on 04/06/2015.
 * Mapper class for sorting giga-level data
 * Input: [ LongWritable, NullWritable ] ---> [ Long type data, null ]
 * Output: [ LongWritable, NullWritable ] ---> Same as input
 */
public class SortMapper extends Mapper<Object, Text, LongWritable, NullWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        long number = Long.parseLong(value.toString());
        LongWritable keyin = new LongWritable(number);
        context.write(keyin, NullWritable.get());
    }
}
