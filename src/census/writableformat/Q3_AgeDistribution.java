package census.writableformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 4/25/15.
 * Custom structure for Question 3
 * Used to calculate age distribution
 */
public class Q3_AgeDistribution extends CensusInfoFormat {

    private final IntWritable type = MessageType.Q3_AgeDistribution;
    private Text state = new Text();
    private LongWritable maleLessThan18 = new LongWritable();
    private LongWritable male19To29 = new LongWritable();
    private LongWritable male30To39 = new LongWritable();
    private LongWritable femaleLessThan18 = new LongWritable();
    private LongWritable female19To29 = new LongWritable();
    private LongWritable female30To39 = new LongWritable();
    private LongWritable totalMale = new LongWritable();
    private LongWritable totalFemale = new LongWritable();

    public Q3_AgeDistribution() {
    }

    public Q3_AgeDistribution(String lineString) {
        state = getStateAbbr(lineString);
        maleLessThan18 = getContinuousFieldSum(lineString, LineIndex.MALE_AGE_LESS_THAN_18_START, LineIndex.MALE_AGE_LESS_THAN_18_FIELDS_COUNT);
        male19To29 = getContinuousFieldSum(lineString, LineIndex.MALE_AGE_19_TO_29_START, LineIndex.MALE_AGE_19_TO_29_FIELDS_COUNT);
        male30To39 = getContinuousFieldSum(lineString, LineIndex.MALE_AGE_30_TO_39_START, LineIndex.MALE_AGE_30_TO_39_FIELDS_COUNT);
        femaleLessThan18 = getContinuousFieldSum(lineString, LineIndex.FEMALE_AGE_LESS_THAN_18_START, LineIndex.FEMALE_AGE_LESS_THAN_18_FIELDS_COUNT);
        female19To29 = getContinuousFieldSum(lineString, LineIndex.FEMALE_AGE_19_TO_29_START, LineIndex.FEMALE_AGE_19_TO_29_FIELDS_COUNT);
        female30To39 = getContinuousFieldSum(lineString, LineIndex.FEMALE_AGE_30_TO_39_START, LineIndex.FEMALE_AGE_30_TO_39_FIELDS_COUNT);
        totalMale = getContinuousFieldSum(lineString, LineIndex.TOTAL_MALE_START, LineIndex.TOTAL_MALE_FIELDS_COUNT);
        totalFemale = getContinuousFieldSum(lineString, LineIndex.TOTAL_FEMALE_START, LineIndex.TOTAL_FEMALE_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getMaleLessThan18() {
        return maleLessThan18;
    }

    public LongWritable getMale19To29() {
        return male19To29;
    }

    public LongWritable getMale30To39() {
        return male30To39;
    }

    public LongWritable getFemaleLessThan18() {
        return femaleLessThan18;
    }

    public LongWritable getFemale19To29() {
        return female19To29;
    }

    public LongWritable getFemale30To39() {
        return female30To39;
    }

    public LongWritable getTotalMale() {
        return totalMale;
    }

    public LongWritable getTotalFemale() {
        return totalFemale;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        maleLessThan18.readFields(in);
        male19To29.readFields(in);
        male30To39.readFields(in);
        femaleLessThan18.readFields(in);
        female19To29.readFields(in);
        female30To39.readFields(in);
        totalMale.readFields(in);
        totalFemale.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        maleLessThan18.write(out);
        male19To29.write(out);
        male30To39.write(out);
        femaleLessThan18.write(out);
        female19To29.write(out);
        female30To39.write(out);
        totalMale.write(out);
        totalFemale.write(out);
    }

    @Override
    public IntWritable getType() {
        return type;
    }

    @Override
    public Text getKey() {
        return new Text("Q3_" + state.toString());
    }
}
