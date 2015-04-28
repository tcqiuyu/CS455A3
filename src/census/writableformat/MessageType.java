package census.writableformat;

import org.apache.hadoop.io.IntWritable;

/**
 * Created by Qiu on 4/25/15.
 * Different message types
 */
public interface MessageType {

    public static final IntWritable Q1_RentAndOwned = new IntWritable(1);
    public static final IntWritable Q2_NeverMarried = new IntWritable(2);
    public static final IntWritable Q3_AgeDistribution = new IntWritable(3);
    public static final IntWritable Q4_RuralAndUrban = new IntWritable(4);
    public static final IntWritable Q5_OwnerOccupiedHouseValue = new IntWritable(5);
    public static final IntWritable Q6_RentPaid = new IntWritable(6);
    public static final IntWritable Q7_RoomNumber = new IntWritable(7);
    public static final IntWritable Q8_Elder = new IntWritable(8);
}
