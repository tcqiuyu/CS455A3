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
public class Q3_AgeDistribution extends CensusInfoFormat {

    private Text state;
    private LongWritable maleLessThan18;
    private LongWritable male19To29;
    private LongWritable male30To39;
    private LongWritable femaleLessThan18;
    private LongWritable female19To29;
    private LongWritable female30To39;
    private IntWritable type = MessageType.Q3_AgeDistribution;

    public Q3_AgeDistribution(String lineString) {
        state = getStateAbbr(lineString);
        maleLessThan18 = getContinuousFieldSum(lineString, LineIndex.MALE_AGE_LESS_THAN_18_START, LineIndex.MALE_AGE_LESS_THAN_18_FIELDS_COUNT);
        male19To29 = getContinuousFieldSum(lineString, LineIndex.MALE_AGE_19_TO_29_START, LineIndex.MALE_AGE_19_TO_29_FIELDS_COUNT);
        male30To39 = getContinuousFieldSum(lineString, LineIndex.MALE_AGE_30_TO_39_START, LineIndex.MALE_AGE_30_TO_39_FIELDS_COUNT);
        femaleLessThan18 = getContinuousFieldSum(lineString, LineIndex.FEMALE_AGE_LESS_THAN_18_START, LineIndex.FEMALE_AGE_LESS_THAN_18_FIELDS_COUNT);
        female19To29 = getContinuousFieldSum(lineString, LineIndex.FEMALE_AGE_19_TO_29_START, LineIndex.FEMALE_AGE_19_TO_29_FIELDS_COUNT);
        female30To39 = getContinuousFieldSum(lineString, LineIndex.FEMALE_AGE_30_TO_39_START, LineIndex.FEMALE_AGE_30_TO_39_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getMaleLessThan18() {
        return maleLessThan18;
    }

    public void setMaleLessThan18(LongWritable maleLessThan18) {
        this.maleLessThan18 = maleLessThan18;
    }

    public LongWritable getMale19To29() {
        return male19To29;
    }

    public void setMale19To29(LongWritable male19To29) {
        this.male19To29 = male19To29;
    }

    public LongWritable getMale30To39() {
        return male30To39;
    }

    public void setMale30To39(LongWritable male30To39) {
        this.male30To39 = male30To39;
    }

    public LongWritable getFemaleLessThan18() {
        return femaleLessThan18;
    }

    public void setFemaleLessThan18(LongWritable femaleLessThan18) {
        this.femaleLessThan18 = femaleLessThan18;
    }

    public LongWritable getFemale19To29() {
        return female19To29;
    }

    public void setFemale19To29(LongWritable female19To29) {
        this.female19To29 = female19To29;
    }

    public LongWritable getFemale30To39() {
        return female30To39;
    }

    public void setFemale30To39(LongWritable female30To39) {
        this.female30To39 = female30To39;
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
    }

    @Override
    public IntWritable getType() {
        return type;
    }
}