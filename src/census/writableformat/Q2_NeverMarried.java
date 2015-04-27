package census.writableformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 4/25/15.
 * Custom structure for Question 2
 * Used to calculate percentage of male/female that are never married
 */

public class Q2_NeverMarried extends CensusInfoFormat {
    private final IntWritable type = MessageType.Q2_NeverMarried;
    private Text state = new Text();
    private LongWritable maleNeverMarried = new LongWritable();
    private LongWritable femaleNeverMarried = new LongWritable();
    private LongWritable totalMale = new LongWritable();
    private LongWritable totalFemale = new LongWritable();

    public Q2_NeverMarried() {
    }


    public Q2_NeverMarried(String lineString) {
        state = getStateAbbr(lineString);
        maleNeverMarried = getContinuousFieldSum(lineString, LineIndex.MALE_NEVER_MARRIED_START, LineIndex.MALE_NEVER_MARRIED_FIELDS_COUNT);
        femaleNeverMarried = getContinuousFieldSum(lineString, LineIndex.FEMALE_NEVER_MARRIED_START, LineIndex.FEMALE_NEVER_MARRIED_FIELDS_COUNT);
        totalMale = getContinuousFieldSum(lineString, LineIndex.TOTAL_MALE_MARITAL_STATUS_START, LineIndex.TOTAL_MALE_MARITAL_STATUS_FIELDS_COUNT);
        totalFemale = getContinuousFieldSum(lineString, LineIndex.TOTAL_FEMALE_MARITAL_STATUS_START, LineIndex.TOTAL_FEMALE_MARITAL_STATUS_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getMaleNeverMarried() {
        return maleNeverMarried;
    }

    public LongWritable getFemaleNeverMarried() {
        return femaleNeverMarried;
    }

    public LongWritable getTotalMale() {
        return totalMale;
    }

    public LongWritable getTotalFemale() {
        return totalFemale;
    }

    @Override
    public IntWritable getType() {
        return type;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        maleNeverMarried.write(out);
        femaleNeverMarried.write(out);
        totalMale.write(out);

        totalFemale.write(out);
        type.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        maleNeverMarried.readFields(in);
        femaleNeverMarried.readFields(in);
        totalMale.readFields(in);
        totalFemale.readFields(in);
        type.readFields(in);
    }

    @Override
    public Text getKey() {
        return new Text("Q2_" + state.toString());
    }
}
