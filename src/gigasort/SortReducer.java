package gigasort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu on 04/06/2015.
 * Combiner class for gigasort job
 */
public class SortReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {

    private static int counter = 0;

    @Override
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable ignored : values) {
            if (counter % 1000 == 0) {
                context.write(key, NullWritable.get());
                counter++;
            }
        }
    }
}
