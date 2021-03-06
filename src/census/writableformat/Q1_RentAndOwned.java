package census.writableformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 4/25/15.
 * Custom Structure for Question 1
 * Used to calculate percentage of rent/owned
 */

public class Q1_RentAndOwned extends CensusInfoFormat {

    private final IntWritable type = MessageType.Q1_RentAndOwned;
    private Text state;
    private LongWritable rent;
    private LongWritable owned;


    public Q1_RentAndOwned() {
        state = new Text();
        rent = new LongWritable();
        owned = new LongWritable();
    }

    public Q1_RentAndOwned(String lineString) {
        state = getStateAbbr(lineString);
        rent = getContinuousFieldSum(lineString, LineIndex.RENTER_OCCUPIED_START, LineIndex.RENTER_OCCUPIED_FIELDS_COUNT);
        owned = getContinuousFieldSum(lineString, LineIndex.OWNER_OCCUPIED_START, LineIndex.OWNER_OCCUPIED_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getRent() {
        return rent;
    }

    public LongWritable getOwned() {
        return owned;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        rent.write(out);
        owned.write(out);
        type.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        rent.readFields(in);
        owned.readFields(in);
        type.readFields(in);
    }

    @Override
    public IntWritable getType() {
        return type;
    }

    @Override
    public Text getKey() {
        return new Text("Q1_" + state.toString());
    }
}
