package census.structure;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Created by Qiu on 4/23/15.
 */
public class LongArrayWritable extends ArrayWritable {

    public LongArrayWritable() {
        super(LongWritable.class);
    }
}
