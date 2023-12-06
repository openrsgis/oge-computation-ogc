package whu.edu.cn.trajectory.db.database.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.db.database.Database;
import whu.edu.cn.trajectory.db.database.meta.DataSetMeta;
import whu.edu.cn.trajectory.db.database.meta.IndexMeta;
import whu.edu.cn.trajectory.db.database.table.IndexTable;
import whu.edu.cn.trajectory.db.database.util.BulkLoadDriverUtils;

import java.util.List;

import static whu.edu.cn.trajectory.db.constant.DBConstants.*;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public class TextBulkLoadDriver extends Configured {

    private static Logger logger = LoggerFactory.getLogger(TextBulkLoadDriver.class);

    static Database instance;

    private void setUpConfiguration(String inPath, String outPath) {
        Configuration conf = getConf();
        conf.set(BULK_LOAD_INPUT_FILE_PATH_KEY, inPath);
        conf.set(BULK_LOAD_TEMP_FILE_PATH_KEY, outPath);
    }


    private void textToMainIndexes(Class parser, DataSetMeta dataSetMeta) throws Exception {
        // 多个dataset meta，先处理其中的主索引，后处理其中的辅助索引
        List<IndexMeta> indexMetaList = dataSetMeta.getIndexMetaList();
        for (IndexMeta im : indexMetaList) {
            if (im.isMainIndex()) {
                long startLoadTime = System.currentTimeMillis();
                logger.info("Starting bulk load main index, meta: {}", im);
                IndexTable indexTable = new IndexTable(im);
                try {
                    BulkLoadDriverUtils.createIndexFromFile(getConf(), indexTable, parser);
                } catch (Exception e) {
                    logger.error("Failed to finish bulk load main index {}", im, e);
                    throw e;
                }
                long endLoadTime = System.currentTimeMillis();
                logger.info("Main index {} load finished, cost time: {}ms.", im.getIndexTableName(), (endLoadTime - startLoadTime));
            }
        }
    }

    private void tableToSecondaryIndexes(DataSetMeta dataSetMeta) throws Exception {
        for (IndexMeta im : dataSetMeta.getIndexMetaList()) {
            if (!im.isMainIndex()) {
                BulkLoadDriverUtils.createIndexFromTable(getConf(), im, dataSetMeta);
            }
        }
    }

    /**
     * 将文本文件中的轨迹数据以MapReduce BulkLoad的形式导入至HBase中，本方法仅适用于全新的数据集。
     * 需要配合{@link Database#createDataSet}使用，该方法后将创建相关索引的空表，本方法负责向空表中写入数据。
     * @param parser 将文本解析为Trajectory对象的实现类
     * @param inPath 输入文件路径，该文件应位于HDFS上
     * @param outPath MapReduce BulkLoad过程中产生的中间文件路径，该路径应位于HDFS上
     * @param dataSetMeta 目标数据集的元信息
     * @throws Exception BulkLoad失败
     */
    public void datasetBulkLoad(Class parser, String inPath, String outPath, DataSetMeta dataSetMeta) throws Exception {
        instance = Database.getInstance();
        logger.info("Starting bulk load dataset {}", dataSetMeta.getDataSetName());
        long start = System.currentTimeMillis();
        setUpConfiguration(inPath, outPath);
        // 利用文本文件将数据导入至所有主数据表中
        textToMainIndexes(parser, dataSetMeta);
        // 利用核心主索引表的数据，导入至所有辅助索引表中
        tableToSecondaryIndexes(dataSetMeta);
        logger.info("All indexes of Dataset [{}] have been loaded into HBase, total time cost: {}ms.",
                dataSetMeta.getDataSetName(),
                System.currentTimeMillis() - start);
    }

    /**
     * 将文本文件中的轨迹数据以MapReduce BulkLoad的形式导入至index meta对应的索引表中。
     * 要求目标数据集已经存在于MetaTable中，index meta信息已写入dataset meta中, HBase table已创建。
     * 需要配合{@link Database#addIndexMeta}使用，该方法中负责将待添加index meta的信息更新至dataSetMeta中，并创建新表，
     * 但不负责数据的实际写入。数据写入由本类负责。
     * @param cls 将文本解析为Trajectory的类对象，需实现{@link TextTrajParser}
     * @param inPath 输入文件路径，该文件应位于HDFS上
     * @param outPath MapReduce BulkLoad过程中产生的中间文件路径，该路径应位于HDFS上
     * @param indexMeta 待写入index的元信息
     * @throws Exception BulkLoad失败
     */
    public void mainIndexBulkLoad(Class cls, String inPath, String outPath, IndexMeta indexMeta) throws Exception {
        if (!indexMeta.isMainIndex()) {
            String msg = String.format("IndexMeta: %s is not main index, can't bulk load in this method.", indexMeta.getIndexTableName());
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }
        logger.info("Starting bulk load index {} into dataset {}", indexMeta.getIndexTableName(), indexMeta.getDataSetName());
        long start = System.currentTimeMillis();
        setUpConfiguration(inPath, outPath);
        // 利用文本文件将数据导入至所有主数据表中
        BulkLoadDriverUtils.createIndexFromFile(getConf(), new IndexTable(indexMeta), cls);
        logger.info("Index [{}] have been loaded into dataset {}, total time cost: {}ms.",
                indexMeta.getIndexTableName(),
                indexMeta.getDataSetName(),
                System.currentTimeMillis() - start);
    }
}
