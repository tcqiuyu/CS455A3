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
    private LongWritable totalMale;
    private LongWritable totalFemale;
    private IntWritable type = MessageType.Q2_NeverMarried;

    public Q2_NeverMarried(String lineString) {
        state = getStateAbbr(lineString);
        maleNeverMarried = getContinuousFieldSum(lineString, LineIndex.MALE_NEVER_MARRIED_START, LineIndex.MALE_NEVER_MARRIED_FIELDS_COUNT);
        femaleNeverMarried = getContinuousFieldSum(lineString, LineIndex.FEMALE_NEVER_MARRIED_START, LineIndex.FEMALE_NEVER_MARRIED_FIELDS_COUNT);
        totalMale = getContinuousFieldSum(lineString, LineIndex.TOTAL_MALE_START, LineIndex.TOTAL_MALE_FIELDS_COUNT);
        totalFemale = getContinuousFieldSum(lineString, LineIndex.TOTAL_FEMALE_START, LineIndex.TOTAL_FEMALE_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getMaleNeverMarried() {
        return maleNeverMarried;
    }

    public void setMaleNeverMarried(LongWritable maleNeverMarried) {
        this.maleNeverMarried = maleNeverMarried;
    }

    public LongWritable getFemaleNeverMarried() {
        return femaleNeverMarried;
    }

    public void setFemaleNeverMarried(LongWritable femaleNeverMarried) {
        this.femaleNeverMarried = femaleNeverMarried;
    }

    @Override
    public IntWritable getType() {
        return type;
    }

    public LongWritable getTotalMale() {
        return totalMale;
    }

    public void setTotalMale(LongWritable totalMale) {
        this.totalMale = totalMale;
    }

    public LongWritable getTotalFemale() {
        return totalFemale;
    }

    public void setTotalFemale(LongWritable totalFemale) {
        this.totalFemale = totalFemale;
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
