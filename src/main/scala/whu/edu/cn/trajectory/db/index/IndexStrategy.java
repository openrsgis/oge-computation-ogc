package whu.edu.cn.trajectory.db.index;

import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.db.coding.coding.SpatialCoding;
import whu.edu.cn.trajectory.db.coding.coding.TimeCoding;
import whu.edu.cn.trajectory.db.constant.IndexConstants;
import whu.edu.cn.trajectory.db.datatypes.ByteArray;
import whu.edu.cn.trajectory.db.datatypes.TimeBin;
import whu.edu.cn.trajectory.db.datatypes.TimeLine;
import whu.edu.cn.trajectory.db.enums.IndexType;
import whu.edu.cn.trajectory.db.query.basic.condition.AbstractQueryCondition;
import whu.edu.cn.trajectory.db.query.basic.condition.SpatialQueryCondition;
import whu.edu.cn.trajectory.db.query.basic.condition.SpatialTemporalQueryCondition;
import whu.edu.cn.trajectory.db.query.basic.condition.TemporalQueryCondition;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static whu.edu.cn.trajectory.db.constant.CodingConstants.MAX_OID_LENGTH;

/**
 * 接收对象,输出row-key.
 * 接收row-key,输出索引信息
 * 接收查询条件, 输出ROW-key范围
 * @author xuqi
 * @date 2023/12/01
 */
public abstract class IndexStrategy implements Serializable {

    protected short shardNum = IndexConstants.DEFAULT_SHARD_NUM;

    protected IndexType indexType;

    /**
     * 获取轨迹在数据库中的物理索引, 即在逻辑索引之前拼接上shard, shard由逻辑索引的hash值作模运算得到。
     */
    public ByteArray index(Trajectory trajectory) {
        ByteArray logicalIndex = logicalIndex(trajectory);
        short shard = (short) Math.abs((logicalIndex.hashCode() % shardNum));
        ByteBuffer buffer = ByteBuffer.allocate(logicalIndex.getBytes().length + Short.BYTES);
        buffer.put(Bytes.toBytes(shard));
        buffer.put(logicalIndex.getBytes());
        return new ByteArray(buffer.array());
    }

    // 对轨迹编码
    protected abstract ByteArray logicalIndex(Trajectory trajectory);

    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * Get RowKey pairs for scan operation, based on spatial and temporal range.
     * A pair of RowKey is the startKey and endKey of a single scan.
     * @param queryCondition 查询框
     * @return RowKey pairs
     */
    public abstract List<RowKeyRange> getScanRanges(AbstractQueryCondition queryCondition);
    public abstract List<RowKeyRange> getScanRanges(AbstractQueryCondition queryCondition, String oid);

    public abstract String parsePhysicalIndex2String(ByteArray byteArray);

    public byte[] getObjectIDBytes(Trajectory trajectory) {
        return getObjectIDBytes(trajectory.getObjectID());
    }

    public byte[] getObjectIDBytes(String oid) {
        return bytePadding(oid.getBytes(StandardCharsets.UTF_8), MAX_OID_LENGTH);
    }

    public byte[] getTrajectoryIDBytes(Trajectory trajectory) {
        return bytePadding(trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8), MAX_OID_LENGTH);
    }

    private byte[] bytePadding(byte[] bytes, int length) {
        byte[] b3 = new byte[length];
        if (bytes.length < length) {
            byte[] bytes1 = new byte[length - bytes.length];
            ByteBuffer buffer = ByteBuffer.allocate(length);
            buffer.put(bytes1);
            buffer.put(bytes);
            b3 = buffer.array();
        } else {
            System.arraycopy(bytes, 0, b3, 0, length);
        }
        return b3;
    }

    public abstract short getShardNum(ByteArray byteArray);
    public abstract String getObjectID(ByteArray byteArray);

    public abstract String getTrajectoryID(ByteArray byteArray);

    /**
     * 为避免hotspot问题, 在index中设计了salt shard.
     * 为进一步确保不同shard的数据分发至不同region server, 需要对相应的索引表作pre-split操作.
     * 本方法根据shard的数量, 生成shard-1个分割点，从而将表pre-splt为shard个region.
     * @return 本索引表的split points.
     */
    public byte[][] getSplits() {
        byte[][] splits = new byte[shardNum - 1][];
        for (short i = 0; i < shardNum - 1; i++) {
            short split = (short) (i + 1);
            byte[] bytes = new byte[2];
            bytes[0] = (byte)((split >> 8) & 0xff);
            bytes[1] = (byte)(split & 0xff);
            splits[i] = bytes;
        }
        return splits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexStrategy that = (IndexStrategy) o;
        return shardNum == that.shardNum && indexType == that.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardNum, indexType);
    }

}
