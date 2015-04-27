package census.mapreduce;

import census.writableformat.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu on 4/23/15.
 */
public class CensusMapper extends Mapper<LongWritable, Text, Text, CensusInfoFormat> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String lineString = value.toString();

        if (getSummaryLevel(lineString) != 100) {
            return;
        }

        int segment = getSegment(lineString);

        switch (segment) {
            case 1:
                Q2_NeverMarried q2 = new Q2_NeverMarried(lineString);
                Q3_AgeDistribution q3 = new Q3_AgeDistribution(lineString);
                Q8_Elder q8 = new Q8_Elder(lineString);
                context.write(q2.getKey(), q2);
                context.write(q3.getKey(), q3);
                context.write(q8.getKey(), q8);
            case 2:
                Q1_RentAndOwned q1 = new Q1_RentAndOwned(lineString);
                Q4_RuralAndUrban q4 = new Q4_RuralAndUrban(lineString);
                Q5_OwnerOccupiedHouseValue q5 = new Q5_OwnerOccupiedHouseValue(lineString);
                Q6_RentPaid q6 = new Q6_RentPaid(lineString);
                Q7_RoomNumber q7 = new Q7_RoomNumber(lineString);
                context.write(q1.getKey(), q1);
                context.write(q4.getKey(), q4);
                context.write(q5.getKey(), q5);
                context.write(q6.getKey(), q6);
                context.write(q7.getKey(), q7);
            default:
        }

    }

    private int getSummaryLevel(String lineString) {
        String summaryLevetStr = lineString.substring(LineIndex.SUMMARY_LEVEL_START, LineIndex.SUMMARY_LEVEL_END);
        return Integer.parseInt(summaryLevetStr);
    }

    private int getSegment(String lineString) {
        String segmentStr = lineString.substring(LineIndex.LOGICAL_RECORD_PART_START, LineIndex.LOGICAL_RECORD_PART_END);
        return Integer.parseInt(segmentStr);
    }
}
