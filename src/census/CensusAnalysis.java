package census;

import census.mapreduce.CensusMapper;
import census.mapreduce.CensusReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by Qiu on 4/23/15.
 */
public class CensusAnalysis {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        Job census_Job = Job.getInstance(configuration, "census");

        census_Job.setJarByClass(CensusAnalysis.class);
        census_Job.setMapperClass(CensusMapper.class);
        census_Job.setReducerClass(CensusReducer.class);


    }

}
