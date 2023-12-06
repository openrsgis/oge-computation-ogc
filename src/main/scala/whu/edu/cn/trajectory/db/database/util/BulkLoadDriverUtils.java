package whu.edu.cn.trajectory.db.database.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.db.constant.DBConstants;
import whu.edu.cn.trajectory.db.database.Database;
import whu.edu.cn.trajectory.db.database.driver.TableBulkLoadDriver;
import whu.edu.cn.trajectory.db.database.driver.TextBulkLoadDriver;
import whu.edu.cn.trajectory.db.database.mapper.MainToMainMapper;
import whu.edu.cn.trajectory.db.database.mapper.MainToSecondaryMapper;
import whu.edu.cn.trajectory.db.database.mapper.TextToMainMapper;
import whu.edu.cn.trajectory.db.database.meta.DataSetMeta;
import whu.edu.cn.trajectory.db.database.meta.IndexMeta;
import whu.edu.cn.trajectory.db.database.table.IndexTable;

import java.io.IOException;

import static org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.DEFAULT_PATH;
import static org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner.PARTITIONER_PATH;
import static whu.edu.cn.trajectory.db.constant.DBConstants.*;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public class BulkLoadDriverUtils {
    private static final Logger logger = LoggerFactory.getLogger(BulkLoadDriverUtils.class);

    private static Database instance;

    static {
        try {
            instance = Database.getInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 本方法以核心索引表中的轨迹为数据源，按照新增IndexMeta转换，并BulkLoad至HBase中。
     * 执行此方法时，应确保DataSetMeta中已有本Secondary Table的信息。
     */
    public static void createIndexFromTable(Configuration conf, IndexMeta indexMeta, DataSetMeta dataSetMeta) throws IOException {
        Path outPath = new Path(conf.get(DBConstants.BULK_LOAD_TEMP_FILE_PATH_KEY));
        String inputTableName = dataSetMeta.getCoreIndexMeta().getIndexTableName();
        String outTableName = indexMeta.getIndexTableName();
        Job job = Job.getInstance(conf, "Batch Import HBase Table：" + outTableName);
        job.setJarByClass(TableBulkLoadDriver.class);
        // 设置MapReduce任务输出的路径
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 配置Map算子，根据待写入Index是否为主索引，选择对应的Mapper实现。
        job.getConfiguration().set(BULKLOAD_TARGET_INDEX_NAME, outTableName);
        job.getConfiguration().setBoolean(ENABLE_SIMPLE_SECONDARY_INDEX, indexMeta.getIndexTableName().contains("simple"));

        if (indexMeta.isMainIndex()) {
            TableMapReduceUtil.initTableMapperJob(inputTableName,
                    buildCoreIndexScan(),
                    MainToMainMapper.class,
                    ImmutableBytesWritable.class,
                    Put.class,
                    job);
        } else {
            TableMapReduceUtil.initTableMapperJob(inputTableName,
                    buildCoreIndexScan(),
                    MainToSecondaryMapper.class,
                    ImmutableBytesWritable.class,
                    Put.class,
                    job);
        }

        // 配置Reduce算子
        job.setReducerClass(PutSortReducer.class);

        RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(outTableName));
        try (Admin admin = instance.getAdmin(); Table table = instance.getTable(outTableName)) {
            HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
            cancelDeleteOnExit(job);
            if (!job.waitForCompletion(true)) {
                return;
            }
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outPath, admin, table, locator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将文本文件中的数据bulk load到索引表中，见{@link TextBulkLoadDriver#mainIndexBulkLoad}.
     * @param conf
     * @param indexTable
     * @throws IOException
     */
    public static void createIndexFromFile(Configuration conf, IndexTable indexTable, Class parser) throws IOException {
        Path inPath = new Path(conf.get(BULK_LOAD_INPUT_FILE_PATH_KEY));
        Path outPath = new Path(conf.get(DBConstants.BULK_LOAD_TEMP_FILE_PATH_KEY));
        String tableName = indexTable.getIndexMeta().getIndexTableName();
        Job job = Job.getInstance(conf, "Batch Import HBase Table：" + tableName);
        job.setJarByClass(TextBulkLoadDriver.class);
        FileInputFormat.setInputPaths(job, inPath);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        // config parser and IndexTable
        job.getConfiguration().setClass(BULKLOAD_TEXT_PARSER_CLASS, parser, TextTrajParser.class);
        job.getConfiguration().set(BULKLOAD_TARGET_INDEX_NAME, tableName);

        // config Map related content
        Class<TextToMainMapper> cls = TextToMainMapper.class;
        job.setMapperClass(cls);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //配置Reduce
        job.setNumReduceTasks(1);
        job.setReducerClass(PutSortReducer.class);

        RegionLocator locator = instance.getConnection().getRegionLocator(TableName.valueOf(tableName));
        try (Admin admin = instance.getAdmin(); Table table = instance.getTable(tableName)) {
            HFileOutputFormat2.setOutputPath(job, outPath);
            HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
            cancelDeleteOnExit(job);
            job.waitForCompletion(true);
            logger.info("HFileOutputFormat2 file ready on {}", outPath);
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outPath, admin, table, locator);
        } catch (Exception e) {
            logger.error("meet error:", e);
            throw new RuntimeException(e);
        }
    }

    public static Scan buildCoreIndexScan() {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(DBConstants.INDEX_TABLE_CF));
        return scan;
    }

    // https://blog.csdn.net/jiandabang/article/details/96483807
    public static void cancelDeleteOnExit(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String partitionsFile = conf.get(PARTITIONER_PATH, DEFAULT_PATH);
        Path partitionsPath = new Path(partitionsFile);
        fs.makeQualified(partitionsPath);
        fs.cancelDeleteOnExit(partitionsPath);
    }

    public static IndexTable getIndexTable(Configuration conf) throws IOException {
        String tableName = conf.get(BULKLOAD_TARGET_INDEX_NAME);
        return new IndexTable(tableName);
    }

    public static TextTrajParser getTextParser(Configuration conf) throws InstantiationException, IllegalAccessException {
        Class cls = conf.getClass(BULKLOAD_TEXT_PARSER_CLASS, null);
        return (TextTrajParser) cls.newInstance();
    }
}
