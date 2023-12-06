package whu.edu.cn.trajectory.db.database.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import whu.edu.cn.trajectory.db.constant.DBConstants;
import whu.edu.cn.trajectory.db.database.Database;
import whu.edu.cn.trajectory.db.database.meta.DataSetMeta;
import whu.edu.cn.trajectory.db.database.meta.IndexMeta;
import whu.edu.cn.trajectory.db.database.util.BulkLoadDriverUtils;

import java.io.IOException;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public class TableBulkLoadDriver extends Configured {

    /**
     * 本方法以核心索引表中的轨迹为数据源，按照新增IndexMeta转换，并BulkLoad至HBase中。
     * 需要配合{@link Database#addIndexMeta}使用，该方法负责将待
     * 添加index meta的信息更新至dataSetMeta中，并创建新表，但不负责数据的实际写入。
     * 数据写入由本类负责。
     */
    public void bulkLoad(String tempOutPutPath, IndexMeta newIndex, DataSetMeta dataSetMeta) throws IOException {
        Configuration conf = getConf();
        conf.set(DBConstants.BULK_LOAD_TEMP_FILE_PATH_KEY, tempOutPutPath);
        BulkLoadDriverUtils.createIndexFromTable(conf, newIndex, dataSetMeta);
    }
}
