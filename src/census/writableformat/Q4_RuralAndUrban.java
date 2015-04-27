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
public class Q4_RuralAndUrban extends CensusInfoFormat {

    private Text state;
    private LongWritable rural;
    private LongWritable urban;
    private IntWritable type = MessageType.Q4_RuralAndUrban;

    public Q4_RuralAndUrban(String lineString) {
        state = getStateAbbr(lineString);
        rural = getContinuousFieldSum(lineString, LineIndex.HOUSE_RURAL_START, LineIndex.HOUSE_RURAL_FIELDS_COUNT);
        urban = getContinuousFieldSum(lineString, LineIndex.HOUSE_URBAN_START, LineIndex.HOUSE_URBAN_FIELDS_COUNT);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getRural() {
        return rural;
    }

    public void setRural(LongWritable rural) {
        this.rural = rural;
    }

    public LongWritable getUrban() {
        return urban;
    }

    public void setUrban(LongWritable urban) {
        this.urban = urban;
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
        type.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        rural.write(out);
        urban.write(out);
        type.write(out);
    }


    @Override
    public Text getKey() {
        return new Text(state.toString() + "_Q4");
    }
}
