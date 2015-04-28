package census.writableformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 4/25/15.
 * Custom structure for Question 4
 * Used to calculate percentage of rural/urban households
 */
public class Q4_RuralAndUrban extends CensusInfoFormat {

    private final IntWritable type = MessageType.Q4_RuralAndUrban;
    private Text state = new Text();
    private LongWritable rural = new LongWritable();
    private LongWritable urban = new LongWritable();
    private LongWritable undefined = new LongWritable();

    public Q4_RuralAndUrban() {
    }

    public Q4_RuralAndUrban(String lineString) {
        state = getStateAbbr(lineString);
        rural = getContinuousFieldSum(lineString, LineIndex.HOUSE_RURAL_START, LineIndex.HOUSE_RURAL_FIELDS_COUNT);
        urban = getContinuousFieldSum(lineString, LineIndex.HOUSE_URBAN_START, LineIndex.HOUSE_URBAN_FIELDS_COUNT);
        undefined = getContinuousFieldSum(lineString, LineIndex.HOUSE_UNDEFINED_START, LineIndex.HOUSE_UNDEFINED_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getRural() {
        return rural;
    }

    public LongWritable getUrban() {
        return urban;
    }

    public LongWritable getUndefined() {
        return undefined;
    }

    @Override
    public IntWritable getType() {
        return type;

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        rural.readFields(in);
        urban.readFields(in);
        undefined.readFields(in);
        type.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        rural.write(out);
        urban.write(out);
        undefined.write(out);
        type.write(out);
    }


    @Override
    public Text getKey() {
        return new Text("Q4_" + state.toString());
    }
}
