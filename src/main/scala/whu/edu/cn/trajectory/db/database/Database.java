package whu.edu.cn.trajectory.db.database;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.db.database.meta.DataSetMeta;
import whu.edu.cn.trajectory.db.database.meta.IndexMeta;
import whu.edu.cn.trajectory.db.database.table.MetaTable;
import whu.edu.cn.trajectory.db.enums.IndexType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static whu.edu.cn.trajectory.db.constant.DBConstants.*;

/**
 * @author xuqi
 * @date 2023/12/03
 */
public final class Database {
    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    private Connection connection;
    private Configuration configuration;
    private volatile static Database instance;

    private Database() {
        configuration = HBaseConfiguration.create();
    }

    public static Database getInstance() throws IOException {
        if (instance == null) {
            synchronized (Database.class) {
                if (instance == null) {
                    instance = new Database();
                }
            }
        }
        instance.openConnection();
        return instance;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    private MetaTable getMetaTable() throws IOException {
        return new MetaTable(getTable(META_TABLE_NAME));
    }

    public boolean dataSetExists(String datasetName) throws IOException {
        MetaTable metaTable = null;
        try {
            metaTable = getMetaTable();
            return metaTable.dataSetExists(datasetName);
        } finally {
            if (metaTable != null) metaTable.close();
        }
    }

    public boolean tableExists(String tableName) throws IOException {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            return admin.tableExists(TableName.valueOf(tableName));
        } finally {
            if (admin != null) admin.close();
        }
    }

    public void createDataSet(DataSetMeta dataSetMeta) throws IOException {
        instance.initDataBase();
        String dataSetName = dataSetMeta.getDataSetName();
        if (dataSetExists(dataSetName)) {
            logger.warn("The dataset(name: {}) already exists.", dataSetName);
            return;
        }
        // put data into dataset meta table
        getMetaTable().putDataSet(dataSetMeta);
        // create one index table for each index meta.
        Map<IndexType, List<IndexMeta>> indexMetaMap = dataSetMeta.getAvailableIndexes();
        for (IndexType indexType : indexMetaMap.keySet()) {
            for (IndexMeta indexMeta : indexMetaMap.get(indexType)) {
                createTable(indexMeta.getIndexTableName(), INDEX_TABLE_CF, indexMeta.getSplits());
            }
        }
        logger.info("Dataset {} created.", dataSetName);
    }

    /**
     * 在原有DataSet的基础之上新加一个IndexMeta，此处仅更改data set meta中的信息，不包含表中数据的导入
     */
    public void addIndexMeta(String dataSetName, IndexMeta newIndexMeta) throws IOException {
        try {
            DataSet dataSet = getDataSet(dataSetName);
            dataSet.addIndexMeta(newIndexMeta);
            // put data into dataset meta table
            getMetaTable().putDataSet(dataSet.getDataSetMeta());
            // create index table for the new index meta.
            createTable(newIndexMeta.getIndexTableName(), INDEX_TABLE_CF, newIndexMeta.getSplits());
            logger.info("Created index table {} for data set {}.", newIndexMeta.getIndexTableName(), dataSetName);
        } catch (Exception e) {
            logger.error("Failed index table {} for data set {}.", newIndexMeta.getIndexTableName(), dataSetName, e);
            throw e;
        }
    }

    public void deleteDataSet(String dataSetName) {
        MetaTable metaTable = null;
        try {
            if (!dataSetExists(dataSetName)) {
                logger.warn("The dataset(name: {}) doesn't exists.", dataSetName);
                return;
            }
            DataSetMeta dataSetMeta = getDataSetMeta(dataSetName);
            // delete all index tables
            Map<IndexType, List<IndexMeta>> indexMetaMap = dataSetMeta.getAvailableIndexes();
            for (IndexType indexType : indexMetaMap.keySet()) {
                for (IndexMeta indexMeta : indexMetaMap.get(indexType)) {
                    deleteTable(indexMeta.getIndexTableName());
                }
            }
            // delete data from dataset meta table
            metaTable = getMetaTable();
            metaTable.deleteDataSetMeta(dataSetName);
            logger.info("Dataset {} deleted.", dataSetName);
        } catch (IOException e) {
            logger.error("Delete dataset {} failed, exception trace: \n {}", dataSetName, e.toString());
        } finally {
            if (metaTable != null) metaTable.close();
        }
    }

    public DataSetMeta getDataSetMeta(String dataSetName) throws IOException {
        MetaTable metaTable = null;
        try {
            metaTable = getMetaTable();
            return metaTable.getDataSetMeta(dataSetName);
        } finally {
            if (metaTable != null) metaTable.close();
        }
    }

    public DataSet getDataSet(String dataSetName) throws IOException {
        return new DataSet(getDataSetMeta(dataSetName));
    }

    /**
     * TODO: close hbase connections
     */
    public void closeConnection() throws IOException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    public void openConnection() throws IOException {
        int threads = Runtime.getRuntime().availableProcessors() * 4;
        ExecutorService service = Executors.newScheduledThreadPool(threads);
        connection = ConnectionFactory.createConnection(configuration, service);
    }

    /**
     * Initiate dataset meta table, if already exists, skip.
     */
    public void initDataBase() throws IOException {
        // 创建DataSetMeta表
        createTable(META_TABLE_NAME, META_TABLE_COLUMN_FAMILY, null);
    }


    /**
     * HBase table options
     */
    public void createTable(String tableName, String columnFamily, byte[][] splits) throws IOException {
        Admin admin = null;
        try {
            if (!tableExists(tableName)) {
                admin = getAdmin();
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
                if (splits != null) {
                    admin.createTable(hTableDescriptor, splits);
                } else {
                    admin.createTable(hTableDescriptor);
                }
            }
        } finally {
            if (admin != null) admin.close();
        }
    }

    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    public Connection getConnection() {
        return connection;
    }

    public void deleteTable(String tableName) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.warn("Table {} not exists, failed to delete it.", tableName);
        }
    }
}
