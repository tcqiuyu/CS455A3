package census.writableformat;

/**
 * Created by Qiu on 4/25/15.
 * Line indices that needed in analysis
 */
public interface LineIndex {

    // general purpose indices
    public static int STATE_START = 8;
    public static int STATE_END = STATE_START + 2;
    public static int SUMMARY_LEVEL_START = 10;
    public static int SUMMARY_LEVEL_END = SUMMARY_LEVEL_START + 3;
    public static int LOGICAL_RECORD_PART_START = 24;
    public static int LOGICAL_RECORD_PART_END = LOGICAL_RECORD_PART_START + 4;

    public static int FIELD_SIZE = 9;

    // Q1
    public static int OWNER_OCCUPIED_START = 1803;
    public static int OWNER_OCCUPIED_FIELDS_COUNT = 1;
    public static int RENTER_OCCUPIED_START = 1812;
    public static int RENTER_OCCUPIED_FIELDS_COUNT = 1;

    // Q2
    public static int MALE_NEVER_MARRIED_START = 4422;
    public static int MALE_NEVER_MARRIED_FIELDS_COUNT = 1;
    public static int FEMALE_NEVER_MARRIED_START = 4467;
    public static int FEMALE_NEVER_MARRIED_FIELDS_COUNT = 1;
    public static int TOTAL_MALE_MARITAL_STATUS_START = 4422;
    public static int TOTAL_MALE_MARITAL_STATUS_FIELDS_COUNT = 4;
    public static int TOTAL_FEMALE_MARITAL_STATUS_START = 4467;
    public static int TOTAL_FEMALE_MARITAL_STATUS_FIELDS_COUNT = 4;

    // Q3
    public static int MALE_AGE_LESS_THAN_18_START = 3864;
    public static int MALE_AGE_LESS_THAN_18_FIELDS_COUNT = 13;
    public static int MALE_AGE_19_TO_29_START = MALE_AGE_LESS_THAN_18_START + MALE_AGE_LESS_THAN_18_FIELDS_COUNT * FIELD_SIZE;
    public static int MALE_AGE_19_TO_29_FIELDS_COUNT = 5;
    public static int MALE_AGE_30_TO_39_START = MALE_AGE_19_TO_29_START + MALE_AGE_19_TO_29_FIELDS_COUNT * FIELD_SIZE;
    public static int MALE_AGE_30_TO_39_FIELDS_COUNT = 2;
    public static int FEMALE_AGE_LESS_THAN_18_START = 4143;
    public static int FEMALE_AGE_LESS_THAN_18_FIELDS_COUNT = 13;
    public static int FEMALE_AGE_19_TO_29_START = FEMALE_AGE_LESS_THAN_18_START + FEMALE_AGE_LESS_THAN_18_FIELDS_COUNT * FIELD_SIZE;
    public static int FEMALE_AGE_19_TO_29_FIELDS_COUNT = 5;
    public static int FEMALE_AGE_30_TO_39_START = FEMALE_AGE_19_TO_29_START + FEMALE_AGE_19_TO_29_FIELDS_COUNT * FIELD_SIZE;
    public static int FEMALE_AGE_30_TO_39_FIELDS_COUNT = 2;
    public static int TOTAL_MALE_START = 3864;
    public static int TOTAL_MALE_FIELDS_COUNT = 31;
    public static int TOTAL_FEMALE_START = 4143;
    public static int TOTAL_FEMALE_FIELDS_COUNT = 31;

    // Q4
    public static int HOUSE_URBAN_START = 1857;
    public static int HOUSE_URBAN_FIELDS_COUNT = 2;
    public static int HOUSE_RURAL_START = 1875;
    public static int HOUSE_RURAL_FIELDS_COUNT = 1;
    public static int HOUSE_UNDEFINED_START = 1884;
    public static int HOUSE_UNDEFINED_FIELDS_COUNT = 1;
    // Q5
    public static int OWNER_OCCUPIED_HOUSE_VALUE_START = 2928;
    public static int OWNER_OCCUPIED_HOUSE_VALUE_FIELDS_COUNT = 20;

    // Q6
    public static int RENT_PAID_START = 3450;
    public static int RENT_PAID_FIELDS_COUNT = 16;

    // Q7
    public static int ROOMS_START = 2388;
    public static int ROOMS_FIELD_COUNT = 9;

    // Q8
    public static int ELDER_PERSON_START = 1065;
    public static int ELDER_PERSON_FIELDS_COUNT = 1;
    public static int TOTAL_PERSON_START = 795;
    public static int TOTAL_PERSON_FIELDS_COUNT = 31;
}
