package census.structure;

import org.apache.hadoop.io.Text;

/**
 * Created by Qiu on 4/27/15.
 * Custom structure to compare percentage of elder per house across all states
 */
public class ElderInfo implements Comparable<ElderInfo> {

    private Text key;
    private Double elderPercent;

    public ElderInfo(Text key, Double elderPercent) {
        this.key = key;
        this.elderPercent = elderPercent;
    }

    public Text getKey() {
        return key;
    }

    public Double getElderPercent() {
        return elderPercent;
    }

    @Override
    public int compareTo(ElderInfo o) {
        return elderPercent.compareTo(o.getElderPercent());
    }

}
