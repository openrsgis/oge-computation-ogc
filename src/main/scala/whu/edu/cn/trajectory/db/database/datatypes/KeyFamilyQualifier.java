package whu.edu.cn.trajectory.db.database.datatypes;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public class KeyFamilyQualifier implements Comparable<KeyFamilyQualifier>, Serializable {

    private byte[] rowKey;
    private byte[] family;
    private byte[] qualifier;


    public KeyFamilyQualifier(byte[] rowKey, byte[] family, byte[] qualifier) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    @Override
    public int compareTo(KeyFamilyQualifier o) {
        int result = Bytes.compareTo(rowKey, o.getRowKey());
        if (result == 0) {
            result = Bytes.compareTo(family, o.getFamily());
            if (result == 0) {
                result = Bytes.compareTo(qualifier, o.getQualifier());
            }
        }
        return result;
    }
}

