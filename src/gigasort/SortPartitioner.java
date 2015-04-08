package gigasort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Qiu on 04/06/2015.
 * Custom partitioner
 * It makes all numbers processed in reducer i are less than the numbers in reducer j, when i < j
 */
public class SortPartitioner extends Partitioner<LongWritable, NullWritable> {

    @Override
    public int getPartition(LongWritable longWritable, NullWritable nullWritable, int numPartitions) {
        Long range = Long.MAX_VALUE / Gigasort.REDUCER_NUM;
        Long key = longWritable.get();
        Double partition = Math.floor(key / range);

        if (partition.intValue() == Gigasort.REDUCER_NUM) {//handles corner case
            return Gigasort.REDUCER_NUM - 1;
        }
        return partition.intValue();
    }
}
