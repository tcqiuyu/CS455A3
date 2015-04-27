package census.writableformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 04/26/2015.
 */
public class Q8_Elder extends CensusInfoFormat {

    private Text state;
    private LongWritable elderCount;
    private LongWritable totalPersonsCount;
    private IntWritable type = MessageType.Q8_Elder;

    public Q8_Elder(String lineString) {
        state = getStateAbbr(lineString);
        elderCount = getContinuousFieldSum(lineString, LineIndex.ELDER_PERSON_START, LineIndex.ELDER_PERSON_FIELDS_COUNT);
        totalPersonsCount = getContinuousFieldSum(lineString, LineIndex.TOTAL_PERSON_START, LineIndex.TOTAL_PERSON_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getElderCount() {

        return elderCount;
    }

    public void setElderCount(LongWritable elderCount) {
        this.elderCount = elderCount;
    }

    public LongWritable getTotalPersonsCount() {
        return totalPersonsCount;
    }

    public void setTotalPersonsCount(LongWritable totalPersonsCount) {
        this.totalPersonsCount = totalPersonsCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        elderCount.write(out);
        totalPersonsCount.write(out);
        type.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        elderCount.readFields(in);
        totalPersonsCount.readFields(in);
        type.readFields(in);
    }

    @Override
    public IntWritable getType() {
        return type;
    }

    @Override
    public Text getKey() {
        return new Text("Q8_" + state.toString());
    }

}
