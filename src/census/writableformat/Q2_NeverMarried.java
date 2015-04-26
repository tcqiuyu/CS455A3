package census.writableformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 4/25/15.
 */
public class Q2_NeverMarried extends CensusInfoFormat {
    private Text state;
    private LongWritable maleNeverMarried;
    private LongWritable femaleNeverMarried;

    public void setFemaleNeverMarried(LongWritable femaleNeverMarried) {
        this.femaleNeverMarried = femaleNeverMarried;
    }

    public void setMaleNeverMarried(LongWritable maleNeverMarried) {
        this.maleNeverMarried = maleNeverMarried;
    }

    private IntWritable type = MessageType.Q2_NeverMarried;

    public Q2_NeverMarried(String lineString) {
        state = getStateAbbr(lineString);
        maleNeverMarried = getContinuousFieldSum(lineString, LineIndex.MALE_NEVER_MARRIED_START, LineIndex.MALE_NEVER_MARRIED_FIELDS_COUNT);
        femaleNeverMarried = getContinuousFieldSum(lineString, LineIndex.FEMALE_NEVER_MARRIED_START, LineIndex.FEMALE_NEVER_MARRIED_FIELDS_COUNT);
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

    @Override
    public IntWritable getType() {
        return type;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
