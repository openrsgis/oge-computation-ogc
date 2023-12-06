package whu.edu.cn.trajectory.db.coding.coding;

import whu.edu.cn.trajectory.db.coding.sfc.SFCRange;
import whu.edu.cn.trajectory.db.datatypes.ByteArray;

import java.nio.ByteBuffer;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class CodingRange {

    ByteArray lower;
    ByteArray upper;
    boolean validated;

    public CodingRange(ByteArray lower, ByteArray upper, boolean validated) {
        this.lower = lower;
        this.upper = upper;
        this.validated = validated;
    }

    public CodingRange() {
    }

    public ByteArray getLower() {
        return lower;
    }

    public ByteArray getUpper() {
        return upper;
    }

    public boolean isValidated() {
        return validated;
    }

    public void concatSfcRange(SFCRange sfcRange) {
        if (lower == null || upper == null) {
            lower = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.lower));
            upper = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.upper));
        } else {
            lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + Long.SIZE / Byte.SIZE)
                    .put(lower.getBytes())
                    .putLong(sfcRange.lower));
            upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + Long.SIZE / Byte.SIZE)
                    .put(upper.getBytes())
                    .putLong(sfcRange.upper));
        }
        validated = sfcRange.validated;
    }

    public void concatTimeIndexRange(SFCRange timeIndexRange) {
        if (lower == null || upper == null) {
            lower = new ByteArray(
                    ByteBuffer.allocate(XZTCoding.BYTES_NUM).putLong(timeIndexRange.getLower()));
            upper = new ByteArray(
                    ByteBuffer.allocate(XZTCoding.BYTES_NUM).putLong(timeIndexRange.getUpper()));
        } else {
            lower = new ByteArray(ByteBuffer.allocate(lower.getBytes().length + XZTCoding.BYTES_NUM)
                    .put(lower.getBytes())
                    .putLong(timeIndexRange.getLower()));
            upper = new ByteArray(ByteBuffer.allocate(upper.getBytes().length + XZTCoding.BYTES_NUM)
                    .put(upper.getBytes())
                    .putLong(timeIndexRange.getUpper()));
        }
        validated = timeIndexRange.isValidated();
    }

    @Override
    public String toString() {
        return "CodingRange{" +
                "lower=" + lower +
                ", upper=" + upper +
                ", contained=" + validated +
                '}';
    }
}
