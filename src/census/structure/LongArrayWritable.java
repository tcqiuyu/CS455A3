package census.structure;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by Qiu on 4/25/15.
 */
public class LongArrayWritable extends ArrayWritable {

    public LongArrayWritable() {
        super(LongWritable.class);
    }

    public LongArrayWritable(LongWritable[] values) {
        super(LongWritable.class, values);
    }
}
