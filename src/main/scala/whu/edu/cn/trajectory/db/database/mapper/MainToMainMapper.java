package whu.edu.cn.trajectory.db.database.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.db.database.table.IndexTable;
import whu.edu.cn.trajectory.db.database.util.TrajectorySerdeUtils;

import java.io.IOException;

import static whu.edu.cn.trajectory.db.database.util.BulkLoadDriverUtils.getIndexTable;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public class MainToMainMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private static IndexTable indexTable;

    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        indexTable = getIndexTable(context.getConfiguration());
    }

    @SuppressWarnings("rawtypes")
    public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job)
            throws IOException {
        TableMapReduceUtil.initTableMapperJob(table, scan, mapper, ImmutableBytesWritable.class, Result.class, job);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result coreIndexRow, Context context) throws IOException, InterruptedException {
        Trajectory t = TrajectorySerdeUtils.getAllTrajectoryFromResult(coreIndexRow);
        Put p = TrajectorySerdeUtils.getPutForMainIndex(indexTable.getIndexMeta(), t);
        context.write(new ImmutableBytesWritable(p.getRow()), p);
    }
}
