package census.writableformat;

import census.structure.LongArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 4/26/15.
 */
public class  Q6_RentPaid extends CensusInfoFormat {

    private Text state;
    private LongArrayWritable rentInfoArray;
    private IntWritable type = MessageType.Q6_RentPaid;

    public Q6_RentPaid(String lineString) {
        state = getStateAbbr(lineString);
        LongWritable[] rentInfos = getFieldArray(lineString, LineIndex.RENT_PAID_START, LineIndex.RENT_PAID_FIELDS_COUNT);
        rentInfoArray = new LongArrayWritable(rentInfos);
    }

    public Text getState() {

        return state;
    }

    public LongArrayWritable getRentInfoArray() {
        return rentInfoArray;
    }

    public void setRentInfoArray(LongArrayWritable rentInfoArray) {
        this.rentInfoArray = rentInfoArray;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        rentInfoArray.readFields(in);
        type.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        rentInfoArray.write(out);
        type.write(out);
    }

    @Override
    public IntWritable getType() {
        return type;
    }
}
