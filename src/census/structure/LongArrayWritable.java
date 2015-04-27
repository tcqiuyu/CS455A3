package census.structure;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

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

    public void addBy(LongArrayWritable other) {

        if (this.get().length == other.get().length) {
            for (int i = 0; i < this.get().length; i++) {
                Long out = ((LongWritable) this.get()[i]).get() + ((LongWritable) other.get()[i]).get();
                this.get()[i] = new LongWritable(out);
            }
        }
    }
}
