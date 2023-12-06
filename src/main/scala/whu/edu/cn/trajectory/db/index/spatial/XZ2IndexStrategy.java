package whu.edu.cn.trajectory.db.index.spatial;

import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.db.coding.coding.CodingRange;
import whu.edu.cn.trajectory.db.coding.coding.SpatialCoding;
import whu.edu.cn.trajectory.db.coding.coding.XZ2Coding;
import whu.edu.cn.trajectory.db.datatypes.ByteArray;
import whu.edu.cn.trajectory.db.datatypes.TimeLine;
import whu.edu.cn.trajectory.db.enums.IndexType;
import whu.edu.cn.trajectory.db.index.IndexStrategy;
import whu.edu.cn.trajectory.db.index.RowKeyRange;
import whu.edu.cn.trajectory.db.query.basic.condition.AbstractQueryCondition;
import whu.edu.cn.trajectory.db.query.basic.condition.SpatialQueryCondition;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajectory.db.constant.CodingConstants.*;

/**
 * row key: shard(short) + xz2(long) + oid(max_oid_length) + tid(max_tid_length)
 * @author xuqi
 * @date 2023/12/01
 */
public class XZ2IndexStrategy extends IndexStrategy {

    private XZ2Coding xz2Coding;

    private static final int PHYSICAL_KEY_BYTE_LEN = Short.BYTES + XZ2Coding.BYTES + MAX_OID_LENGTH + MAX_TID_LENGTH;
    private static final int LOGICAL_KEY_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - Short.BYTES;
    private static final int SCAN_RANGE_BYTE_LEN =  PHYSICAL_KEY_BYTE_LEN - MAX_OID_LENGTH - MAX_TID_LENGTH;

    public XZ2IndexStrategy() {
        indexType = IndexType.XZ2;
        this.xz2Coding = new XZ2Coding();
    }

    @Override
    protected ByteArray logicalIndex(Trajectory trajectory) {
        ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
        ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN);
        byteBuffer.put(spatialCoding.getBytes());
        byteBuffer.put(getObjectIDBytes(trajectory));
        byteBuffer.put(getTrajectoryIDBytes(trajectory));
        return new ByteArray(byteBuffer);
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.XZ2;
    }


    public List<RowKeyRange> getScanRanges(SpatialQueryCondition spatialQueryCondition,
                                           int maxRangeNum) {
        List<RowKeyRange> result = new ArrayList<>();
        // 1. xz2 coding
        List<CodingRange> codingRanges = xz2Coding.ranges(spatialQueryCondition);
        // 2. concat shard index
        for (CodingRange xz2Coding : codingRanges) {
            for (short shard = 0; shard < shardNum; shard++) {
                result.add(new RowKeyRange(toRowKeyRangeBoundary(shard, xz2Coding.getLower(), false),
                        toRowKeyRangeBoundary(shard, xz2Coding.getUpper(), true), xz2Coding.isValidated()));
            }
        }
        return result;
    }

    @Override
    public List<RowKeyRange> getScanRanges(AbstractQueryCondition queryCondition) {
        if(queryCondition instanceof SpatialQueryCondition){
            SpatialQueryCondition spatialQueryCondition = (SpatialQueryCondition) queryCondition;
            return getScanRanges(spatialQueryCondition, 500);
        }else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public List<RowKeyRange> getScanRanges(AbstractQueryCondition queryCondition, String oid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String parsePhysicalIndex2String(ByteArray byteArray) {
        return "Row key index: {" + "shardNum=" + getShardNum(byteArray) + ", xz2="
                + extractSpatialCode(byteArray) +", oidAndTid=" + getObjectID(byteArray) + "-" + getTrajectoryID(byteArray) + '}';
    }

    public SpatialCoding getSpatialCoding() {
        return xz2Coding;
    }

    public ByteArray extractSpatialCode(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        ((Buffer) buffer).flip();
        buffer.getShort();
        byte[] bytes = new byte[XZ2Coding.BYTES];
        buffer.get(bytes);
        return new ByteArray(bytes);
    }

    @Override
    public short getShardNum(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        ((Buffer) buffer).flip();
        return buffer.getShort();
    }

    @Override
    public String getObjectID(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        ((Buffer) buffer).flip();
        buffer.getShort();
        buffer.getLong();
        byte[] stringBytes = new byte[MAX_OID_LENGTH];
        buffer.get(stringBytes);
        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    @Override
    public String getTrajectoryID(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        ((Buffer) buffer).flip();
        buffer.getShort();
        buffer.getLong();
        byte[] oidBytes = new byte[MAX_OID_LENGTH];
        buffer.get(oidBytes);
        byte[] tidBytes = new byte[MAX_TID_LENGTH];
        buffer.get(tidBytes);
        return new String(tidBytes, StandardCharsets.UTF_8);
    }

    private ByteArray toRowKeyRangeBoundary(short shard, ByteArray xz2coding, Boolean isEndCoding) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
        byteBuffer.putShort(shard);
        if (isEndCoding) {
            long xz2code = Bytes.toLong(xz2coding.getBytes()) + 1;
            byteBuffer.putLong(xz2code);
        } else {
            byteBuffer.put(xz2coding.getBytes());
        }
        return new ByteArray(byteBuffer);
    }

    @Override
    public String toString() {
        return "XZ2IndexStrategy{" + "shardNum=" + shardNum + ", indexType=" + indexType
                + ", xz2Coding=" + xz2Coding + '}';
    }
}

