package whu.edu.cn.trajectory.db.database;

import whu.edu.cn.trajectory.db.database.meta.DataSetMeta;
import whu.edu.cn.trajectory.db.database.meta.IndexMeta;
import whu.edu.cn.trajectory.db.database.table.IndexTable;

import java.io.IOException;

/**
 * @author xuqi
 * @date 2023/12/03
 */
public class DataSet {

    private DataSetMeta dataSetMeta;

    public DataSet(DataSetMeta dataSetMeta) {
        this.dataSetMeta = dataSetMeta;
    }

    public String getName() {
        return dataSetMeta.getDataSetName();
    }

    public IndexTable getCoreIndexTable() throws IOException {
        return new IndexTable(dataSetMeta.getCoreIndexMeta());
    }

    public String getCoreIndexName() throws IOException {
        return getCoreIndexTable().getIndexMeta().getIndexTableName();
    }

    public IndexTable getIndexTable(IndexMeta indexMeta) throws IOException {
        return new IndexTable(indexMeta);
    }

    public IndexTable getIndexTable(String tableName) throws IOException {
        return getIndexTable(DataSetMeta.getIndexMetaByName(dataSetMeta.getIndexMetaList(), tableName));
    }

    public DataSetMeta getDataSetMeta() {
        return dataSetMeta;
    }

    // TODO
    public boolean existsIndexMeta(IndexMeta im) {
        return false;
    }

    public void addIndexMeta(IndexMeta indexMeta) {
        dataSetMeta.addIndexMeta(indexMeta);
    }

    public void deleteIndexMeta(String indexName) {
        dataSetMeta.deleteIndex(indexName);
    }
}
