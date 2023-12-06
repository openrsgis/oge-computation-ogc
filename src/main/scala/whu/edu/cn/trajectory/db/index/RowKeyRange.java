package whu.edu.cn.trajectory.db.index;

import whu.edu.cn.trajectory.db.datatypes.ByteArray;

/**
 * A range represents some continuous row keys, and the range will be constructed into an HBase scan object.
 * RowKeyRange对象的startKey为Included，而endKey为Excluded（endKey为大于startKey的最小不符合条件的key，不能included）
 * @author xuqi
 * @date 2023/12/01
 */
public class RowKeyRange {
    ByteArray startKey;
    ByteArray endKey;
    boolean validate;

    public RowKeyRange(ByteArray startKey, ByteArray endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public RowKeyRange(ByteArray startKey, ByteArray endKey, boolean validated) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.validate = validated;
    }

    public ByteArray getStartKey() {
        return startKey;
    }

    public ByteArray getEndKey() {
        return endKey;
    }

    public boolean isValidate() {
        return validate;
    }

    @Override
    public String toString() {
        return "RowKeyRange{" +
                "startKey=" + startKey +
                ", endKey=" + endKey +
                '}';
    }
}
