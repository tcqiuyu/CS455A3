package census.writableformat;

import org.apache.hadoop.io.*;

/**
 * Created by Qiu on 4/25/15.
 */
public abstract class CensusInfoFormat extends GenericWritable {

    public static Class[] CLASSES = new Class[]{
            Q1_RentAndOwned.class,
            Q2_NeverMarried.class,
            Q3_AgeDistribution.class,
            Q4_RuralAndUrban.class,
            Q5_OwnerOccupiedHouseValue.class,
    };

    public abstract IntWritable getType();


    protected Text getStateAbbr(String lineString) {
        String stateStr = lineString.substring(LineIndex.STATE_START, LineIndex.STATE_END);
        return new Text(stateStr);
    }

    protected LongWritable[] getFieldArray(String lineString, int startIdx, int fieldsCount) {
        LongWritable[] output = new LongWritable[fieldsCount];
        for (int i = 0; i < output.length; i++) {
            int start = startIdx + i * LineIndex.FIELD_SIZE;
            int end = start + LineIndex.FIELD_SIZE;
            output[i] = new LongWritable(Long.parseLong(lineString.substring(start, end)));
        }
        return output;
    }

    protected LongWritable getContinuousFieldSum(String lineString, int startIdx, int fieldsCount) {
        Long output = 0L;
        for (int i = 0; i < fieldsCount; i++) {
            int start = startIdx + i * LineIndex.FIELD_SIZE;
            int end = start + LineIndex.FIELD_SIZE;
            output += Long.parseLong(lineString.substring(start, end));
        }
        return new LongWritable(output);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
}
