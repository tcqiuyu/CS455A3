package census.writableformat;

import census.structure.LongArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 4/25/15.
 */
public class Q5_OwnerOccupiedHouseValue extends CensusInfoFormat {

    private Text state;
    private LongArrayWritable values;
    private IntWritable type = MessageType.Q5_OwnerOccupiedHouseValue;

    public Q5_OwnerOccupiedHouseValue(String lineString) {
        state = getStateAbbr(lineString);
        LongWritable[] vals = getFieldArray(lineString,
                LineIndex.OWNER_OCCUPIED_HOUSE_VALUE_START, LineIndex.OWNER_OCCUPIED_HOUSE_VALUE_FIELDS_COUNT);
        values = new LongArrayWritable(vals);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        values.readFields(in);
        type.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        values.write(out);
        type.write(out);
    }

    @Override
    public IntWritable getType() {
        return type;
    }

    public LongArrayWritable getValues() {
        return values;
    }

    public void setValues(LongArrayWritable values) {
        this.values = values;
    }
}
