package census.mapreduce;

import census.writableformat.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Qiu on 4/23/15.
 */
public class CensusReducer extends Reducer<Text, CensusInfoFormat, Text, Text> {

    private MultipleOutputs<Text, Text> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    @Override
    protected void reduce(Text key, Iterable<CensusInfoFormat> values, Context context) throws IOException, InterruptedException {

        String filename = getFilename(key);
        switch (filename) {
            case "Q1":
                Long rent = 0L;
                Long own = 0L;
                for (CensusInfoFormat val : values) {
                    Q1_RentAndOwned actualVal = (Q1_RentAndOwned) val;
                    rent += actualVal.getRent().get();
                    own += actualVal.getOwned().get();
                }
                Double rentPercent = rent / (double) (rent + own);
                Double ownPercent = own / (double) (rent + own);
                mos.write(getFilename(key), key, new Text("Rent: " + rentPercent + "; Own:" + ownPercent));
            case "Q2":
                Long maleNeverMarried = 0L;
                Long femaleNeverMarried = 0L;
                Long totalMale = 0L;
                Long totalFemale = 0L;
                for (CensusInfoFormat val : values) {
                    Q2_NeverMarried actualVal = (Q2_NeverMarried) val;
                    maleNeverMarried += actualVal.getMaleNeverMarried().get();
                    femaleNeverMarried += actualVal.getFemaleNeverMarried().get();
                    totalMale += actualVal.getTotalMale().get();
                    totalFemale += actualVal.getTotalFemale().get();
                }
                Double maleNeverMarriedPercent = maleNeverMarried / (double) totalMale;
                Double femaleNeverMarriedPercent = femaleNeverMarried / (double) totalFemale;
                mos.write(getFilename(key), key, new Text("Male never married: " + maleNeverMarriedPercent + "; Female never married: " + femaleNeverMarriedPercent));
            case "Q3":
                Long maleLessThan18 = 0L;
                Long male19To29 = 0L;
                Long male30To39 = 0L;
                Long femaleLessThan18 = 0L;
                Long female19To29 = 0L;
                Long female30To39 = 0L;
                Long totalMale2 = 0L;
                Long totalFemale2 = 0L;
                for (CensusInfoFormat val : values) {
                    Q3_AgeDistribution actualVal = (Q3_AgeDistribution) val;
                    maleLessThan18 += actualVal.getMaleLessThan18().get();
                    male19To29 += actualVal.getMale19To29().get();
                    male30To39 += actualVal.getMale30To39().get();
                    femaleLessThan18 += actualVal.getFemaleLessThan18().get();
                    female19To29 += actualVal.getFemale19To29().get();
                    female30To39 += actualVal.getFemale30To39().get();
                    totalMale2 += actualVal.getTotalMale().get();
                    totalFemale2 += actualVal.getTotalFemale().get();
                }
                Double maleLessThan18Percent = maleLessThan18 / (double) totalMale2;
                Double male19To29Percent = male19To29 / (double) totalMale2;
                Double male30To39Percent = male30To39 / (double) totalMale2;
                Double femaleLessThan18Percent = femaleLessThan18 / (double) totalFemale2;
                Double female19To29Percent = female19To29 / (double) totalFemale2;
                Double female30To39Percent = female30To39 / (double) totalFemale2;
                mos.write(getFilename(key), key, new Text("Male below 18: " + maleLessThan18Percent + "; Male between 19 and 29: " + male19To29Percent
                        + "; Male between 30 and 39: " + male30To39Percent + "; Female below 18: " + femaleLessThan18Percent
                        + "; Female between 19 and 29: " + female19To29Percent + "; Female between 30 and 39: "
                        + female30To39Percent));
            case "Q4":
                Long rural = 0L;
                Long urban = 0L;
                for (CensusInfoFormat val : values) {
                    Q4_RuralAndUrban actualVal = (Q4_RuralAndUrban) val;
                    rural += actualVal.getRural().get();
                    urban += actualVal.getUrban().get();
                }
                Double ruralPercent = rural / (double) (rural + urban);
                Double urbanPercent = urban / (double) (rural + urban);
                mos.write(getFilename(key), key, new Text("Rural: " + ruralPercent + "; Urban: " + urbanPercent));
            case "Q5":
            case "Q6":
            case "Q7":
            case "Q8":
            default:

        }


    }

    private String getFilename(Text key) {
        String keyStr = key.toString();
        return keyStr.split("_")[0];
    }
}
