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
public class Q7_RoomNumber extends CensusInfoFormat {

    private Text state;
    private LongWritable roomCount;
    private LongWritable houseCount;
    private IntWritable type = MessageType.Q7_RoomNumber;


    public Q7_RoomNumber(String lineString) {
        state = getStateAbbr(lineString);

        LongWritable[] rooms = getFieldArray(lineString, LineIndex.ROOMS_START, LineIndex.ROOMS_FIELD_COUNT);
        long totalrooms = 0L;
        long totalHouses = 0L;
        for (int i = 0; i < rooms.length; i++) {
            totalrooms += rooms[i].get() * (i + 1);
            totalHouses += rooms[i].get();
        }
        roomCount = new LongWritable(totalrooms);
        houseCount = new LongWritable(totalHouses);
    }

    public Text getState() {
        return state;
    }

    public LongWritable getRoomCount() {

        return roomCount;
    }

    public void setRoomCount(LongWritable roomCount) {
        this.roomCount = roomCount;
    }

    public LongWritable getHouseCount() {
        return houseCount;
    }

    public void setHouseCount(LongWritable houseCount) {
        this.houseCount = houseCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        roomCount.write(out);
        houseCount.write(out);
        type.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        roomCount.readFields(in);
        houseCount.readFields(in);
        type.readFields(in);
    }

    @Override
    public IntWritable getType() {
        return type;
    }
}
