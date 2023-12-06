package whu.edu.cn.trajectory.db.database.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.locationtech.jts.io.ParseException;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.db.database.meta.IndexMeta;
import whu.edu.cn.trajectory.db.database.table.IndexTable;
import whu.edu.cn.trajectory.db.database.util.TextTrajParser;
import whu.edu.cn.trajectory.db.database.util.TrajectorySerdeUtils;

import java.io.IOException;

import static whu.edu.cn.trajectory.db.database.util.BulkLoadDriverUtils.getIndexTable;
import static whu.edu.cn.trajectory.db.database.util.BulkLoadDriverUtils.getTextParser;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public class TextToMainMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private static IndexTable indexTable;
    private static TextTrajParser parser;


    @Override
    protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            indexTable = getIndexTable(context.getConfiguration());
            parser = getTextParser(context.getConfiguration());
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String lineValue = value.toString();
        Trajectory trajectory;
        try {
            trajectory = parser.parse(lineValue);
            IndexMeta indexMeta = indexTable.getIndexMeta();
            Put put = TrajectorySerdeUtils.getPutForMainIndex(indexMeta, trajectory);
            context.write(new ImmutableBytesWritable(put.getRow()), put);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
