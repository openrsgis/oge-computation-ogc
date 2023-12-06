package whu.edu.cn.trajectory.db.database.datatypes;

import org.apache.hadoop.hbase.KeyValue;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public class KeyValueInfo {
    private String qualifier;
    private KeyValue value;

    public KeyValueInfo(String qualifier, KeyValue value) {
        this.qualifier = qualifier;
        this.value = value;
    }

    public String getQualifier() {
        return qualifier;
    }

    public KeyValue getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KeyValueInfo{" +
                "qualifier='" + qualifier + '\'' +
                '}';
    }
}
