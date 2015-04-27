package census.mapreduce;

import census.structure.LongArrayWritable;
import census.writableformat.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
                    Q1_RentAndOwned actualVal = (Q1_RentAndOwned) val.get();
                    rent += actualVal.getRent().get();
                    own += actualVal.getOwned().get();
                }
                Double rentPercent = rent / (double) (rent + own);
                Double ownPercent = own / (double) (rent + own);
                mos.write(getFilename(key), key, new Text("Rent: " + rentPercent + "; Own:" + ownPercent));
                break;
            case "Q2":
                Long maleNeverMarried = 0L;
                Long femaleNeverMarried = 0L;
                Long totalMaleMarital = 0L;
                Long totalFemaleMartial = 0L;
                for (CensusInfoFormat val : values) {
                    Q2_NeverMarried actualVal = (Q2_NeverMarried) val.get();
                    maleNeverMarried += actualVal.getMaleNeverMarried().get();
                    femaleNeverMarried += actualVal.getFemaleNeverMarried().get();
                    totalMaleMarital += actualVal.getTotalMale().get();
                    totalFemaleMartial += actualVal.getTotalFemale().get();
                }
                Double maleNeverMarriedPercent = maleNeverMarried / (double) totalMaleMarital;
                Double femaleNeverMarriedPercent = femaleNeverMarried / (double) totalFemaleMartial;
                mos.write(getFilename(key), key, new Text("Male never married: " + maleNeverMarriedPercent + "; Female never married: " + femaleNeverMarriedPercent));
                break;
            case "Q3":
                Long maleLessThan18 = 0L;
                Long male19To29 = 0L;
                Long male30To39 = 0L;
                Long femaleLessThan18 = 0L;
                Long female19To29 = 0L;
                Long female30To39 = 0L;
                Long totalMale = 0L;
                Long totalFemale = 0L;
                for (CensusInfoFormat val : values) {
                    Q3_AgeDistribution actualVal = (Q3_AgeDistribution) val.get();
                    maleLessThan18 += actualVal.getMaleLessThan18().get();
                    male19To29 += actualVal.getMale19To29().get();
                    male30To39 += actualVal.getMale30To39().get();
                    femaleLessThan18 += actualVal.getFemaleLessThan18().get();
                    female19To29 += actualVal.getFemale19To29().get();
                    female30To39 += actualVal.getFemale30To39().get();
                    totalMale += actualVal.getTotalMale().get();
                    totalFemale += actualVal.getTotalFemale().get();
                }
                Double maleLessThan18Percent = maleLessThan18 / (double) totalMale;
                Double male19To29Percent = male19To29 / (double) totalMale;
                Double male30To39Percent = male30To39 / (double) totalMale;
                Double femaleLessThan18Percent = femaleLessThan18 / (double) totalFemale;
                Double female19To29Percent = female19To29 / (double) totalFemale;
                Double female30To39Percent = female30To39 / (double) totalFemale;
                mos.write(getFilename(key), key, new Text("Male below 18: " + maleLessThan18Percent + "; Male between 19 and 29: " + male19To29Percent
                        + "; Male between 30 and 39: " + male30To39Percent + "; Female below 18: " + femaleLessThan18Percent
                        + "; Female between 19 and 29: " + female19To29Percent + "; Female between 30 and 39: "
                        + female30To39Percent));
                break;
            case "Q4":
                Long rural = 0L;
                Long urban = 0L;
                for (CensusInfoFormat val : values) {
                    Q4_RuralAndUrban actualVal = (Q4_RuralAndUrban) val.get();
                    rural += actualVal.getRural().get();
                    urban += actualVal.getUrban().get();
                }
                Double ruralPercent = rural / (double) (rural + urban);
                Double urbanPercent = urban / (double) (rural + urban);
                mos.write(getFilename(key), key, new Text("Rural: " + ruralPercent + "; Urban: " + urbanPercent));
                break;
            case "Q5":
                LongWritable[] tmpin = new LongWritable[LineIndex.OWNER_OCCUPIED_HOUSE_VALUE_FIELDS_COUNT];

                //initiate array
                for (int i = 0; i < tmpin.length; i++) {
                    tmpin[i] = new LongWritable(0);
                }
                LongArrayWritable houseValueArray = new LongArrayWritable(tmpin);
                for (CensusInfoFormat val : values) {
                    Q5_OwnerOccupiedHouseValue actualVal = (Q5_OwnerOccupiedHouseValue) val.get();
                    houseValueArray.addBy(actualVal.getValues());
                }
                int medianSectionForHouseValue = findMedianInterval(houseValueArray);
                String sectionDescForHouseValue = getHouseValueString(medianSectionForHouseValue);
                mos.write(getFilename(key), key, new Text("Median value of owner occupied house is section " + medianSectionForHouseValue + ": " + sectionDescForHouseValue));
                break;
            case "Q6":
                LongWritable[] tmpin2 = new LongWritable[LineIndex.RENT_PAID_FIELDS_COUNT];

                // initiate array
                for (int i = 0; i < tmpin2.length; i++) {
                    tmpin2[i] = new LongWritable(0);
                }
                LongArrayWritable rentPaidArray = new LongArrayWritable(tmpin2);
                for (CensusInfoFormat val : values) {
                    Q6_RentPaid actualVal = (Q6_RentPaid) val.get();
                    rentPaidArray.addBy(actualVal.getRentInfoArray());
                }
                int medianSectionForRentPaid = findMedianInterval(rentPaidArray);
                String sectionDescForRentPaid = getRentPaidString(medianSectionForRentPaid);
                mos.write(getFilename(key), key, new Text("Median value of rent paid is section " + medianSectionForRentPaid + ": " + sectionDescForRentPaid));
                break;
            case "Q7":
                Long totalRooms = 0L;
                Long totalHouses = 0L;
                for (CensusInfoFormat val : values) {
                    Q7_RoomNumber actualVal = (Q7_RoomNumber) val.get();
                    totalRooms += actualVal.getRoomCount().get();
                    totalHouses += actualVal.getHouseCount().get();
                }
                Double avgRooms = totalRooms / (double) totalHouses;
                mos.write(getFilename(key), key, new Text("Average number of rooms per house: " + avgRooms));
                break;
            case "Q8":
                Long elder = 0L;
                Long totalPerson = 0L;
                for (CensusInfoFormat val : values) {
                    Q8_Elder actualVal = (Q8_Elder) val.get();
                    elder += actualVal.getElderCount().get();
                    totalPerson += actualVal.getTotalPersonsCount().get();
                }
                Double elderPercent = elder / (double) totalPerson;
                mos.write((getFilename(key)), key, new Text("Percentage of elderly people: " + elderPercent));
            default:
        }

    }

    private String getRentPaidString(int section) {
        switch (section + 1) {
            case 1:
                return "Less than $100";
            case 2:
                return "$100 to $149";
            case 3:
                return "$150 to $199";
            case 4:
                return "$200 to $249";
            case 5:
                return "$250 to $299";
            case 6:
                return "$300 to $349";
            case 7:
                return "$350 to $399";
            case 8:
                return "$400 to $449";
            case 9:
                return "$450 to $499";
            case 10:
                return "$500 to $549";
            case 11:
                return "$550 to $599";
            case 12:
                return "$600 to $649";
            case 13:
                return "$650 to $699";
            case 14:
                return "$700 to $749";
            case 15:
                return "$750 to $999";
            case 16:
                return "$1000 or more";
            default:
                return null;
        }
    }

    private String getHouseValueString(int section) {
        switch (section + 1) {
            case 1:
                return "Less than $15,000";
            case 2:
                return "$15,000 - $19,999";
            case 3:
                return "$20,000 - $24,999";
            case 4:
                return "$25,000 - $29,999";
            case 5:
                return "$30,000 - $34,999";
            case 6:
                return "$35,000 - $39,999";
            case 7:
                return "$40,000 - $44,999";
            case 8:
                return "$45,000 - $49,999";
            case 9:
                return "$50,000 - $59,999";
            case 10:
                return "$60,000 - $74,999";
            case 11:
                return "$75,000 - $99,999";
            case 12:
                return "$100,000 - $124,999";
            case 13:
                return "$125,000 - $149,999";
            case 14:
                return "$150,000 - $174,999";
            case 15:
                return "$175,000 - $199,999";
            case 16:
                return "$200,000 - $249,999";
            case 17:
                return "$250,000 - $299,999";
            case 18:
                return "$300,000 - $399,999";
            case 19:
                return "$400,000 - $499,999";
            case 20:
                return "$500,000 or more";
            default:
                return null;
        }
    }

    private int findMedianInterval(LongArrayWritable input) {
        Writable[] inputArr = input.get();

        // find total count
        Long sum = 0L;
        for (int i = 0; i < inputArr.length; i++) {
            sum += ((LongWritable) inputArr[i]).get();
        }

        // calculate median value
        Long median;
        if (sum % 2 != 0) {
            Double d = Math.ceil(sum / 2.0);
            median = d.longValue();
        } else {
            median = sum / 2 + 1;
        }

        // find median value's location
        Long target = 0L;
        for (int i = 0; i < inputArr.length; i++) {
            if (target >= median) {
                return i;
            } else {
                target += ((LongWritable) inputArr[i]).get();
            }

        }
        return -1;
    }


    private String getFilename(Text key) {
        String keyStr = key.toString();
        return keyStr.split("_")[0];
    }
}
