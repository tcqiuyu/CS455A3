package census.structure;

import org.apache.hadoop.io.Text;

/**
 * Created by Qiu on 4/27/15.
 * Custom structure to compare average rooms per house across all states
 */
public class AverageRoomsInfo implements Comparable<AverageRoomsInfo> {

    private Text key;
    private Double averageRooms;

    public AverageRoomsInfo(Text key, double averageRooms) {
        this.averageRooms = averageRooms;
        this.key = key;
    }

    public Double getAverageRooms() {
        return averageRooms;
    }

    public Text getKey() {
        return key;
    }

    @Override
    public int compareTo(AverageRoomsInfo o) {
        return averageRooms.compareTo(o.getAverageRooms());
    }
}
