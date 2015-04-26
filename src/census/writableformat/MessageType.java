package census.writableformat;

import org.apache.hadoop.io.IntWritable;

/**
 * Created by Qiu on 4/25/15.
 */
public interface MessageType {

    public static IntWritable Q1_RentAndOwned = new IntWritable(1);
    public static IntWritable Q2_NeverMarried = new IntWritable(2);
    public static IntWritable Q3_AgeDistribution = new IntWritable(3);
    public static IntWritable Q4_RuralAndUrban = new IntWritable(4);
    public static IntWritable Q5_OwnerOccupiedHouseValue = new IntWritable(5);
    public static IntWritable Q6_RentPaid = new IntWritable(6);
}
