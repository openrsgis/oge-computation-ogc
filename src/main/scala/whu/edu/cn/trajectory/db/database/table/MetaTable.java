package whu.edu.cn.trajectory.db.database.table;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.base.mbr.MinimumBoundingBox;
import whu.edu.cn.trajectory.base.util.SerializerUtils;
import whu.edu.cn.trajectory.db.database.meta.DataSetMeta;
import whu.edu.cn.trajectory.db.database.meta.IndexMeta;
import whu.edu.cn.trajectory.db.database.meta.SetMeta;
import whu.edu.cn.trajectory.db.datatypes.TimeLine;
import whu.edu.cn.trajectory.db.enums.IndexType;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static whu.edu.cn.trajectory.db.constant.DBConstants.*;
import static whu.edu.cn.trajectory.db.constant.SetConstants.*;

/**
 * @author xuqi
 * @date 2023/12/03
 */
public class MetaTable {

  private static Logger logger = LoggerFactory.getLogger(MetaTable.class);

  private Table dataSetMetaTable;

  public MetaTable(Table dataSetMetaTable) {
    this.dataSetMetaTable = dataSetMetaTable;
  }

  public DataSetMeta getDataSetMeta(String dataSetName) throws IOException {
    if (!dataSetExists(dataSetName)) {
      String msg = String.format("Dataset meta of [%s] does not exist", dataSetName);
      logger.error(msg);
      throw new IOException(msg);
    } else {
      Result result = dataSetMetaTable.get(new Get(dataSetName.getBytes()));
      return fromResult(result);
    }
  }

  public boolean dataSetExists(String datasetName) throws IOException {
    return dataSetMetaTable.exists(new Get(datasetName.getBytes()));
  }

  public void putDataSet(DataSetMeta dataSetMeta) throws IOException {
    dataSetMetaTable.put(getPut(dataSetMeta));
  }

  public void deleteDataSetMeta(String datasetName) throws IOException {
    dataSetMetaTable.delete(new Delete(datasetName.getBytes()));
    logger.info(
        String.format("Meta data of dataset [%s] has been removed from meta table", datasetName));
  }

  public List<DataSetMeta> listDataSetMeta() throws IOException {
    Scan scan = new Scan();
    ResultScanner scanner = dataSetMetaTable.getScanner(scan);
    ArrayList<DataSetMeta> dataSetMetas = new ArrayList<>();
    for (Result result = scanner.next(); result != null; result = scanner.next()) {
      DataSetMeta dataSetMeta = fromResult(result);
      dataSetMetas.add(dataSetMeta);
    }
    return dataSetMetas;
  }

  /** Convert DataSetMeta object into HBase Put object for dataset creation. */
  private Put getPut(DataSetMeta dataSetMeta) throws IOException {
    String dataSetName = dataSetMeta.getDataSetName();
    Map<IndexType, List<IndexMeta>> indexMetaMap = dataSetMeta.getAvailableIndexes();
    // row key - 数据集名称
    Put p = new Put(Bytes.toBytes(dataSetName));
    List<IndexMeta> indexMetaList = new ArrayList<>();
    for (IndexType type : indexMetaMap.keySet()) {
      indexMetaList.addAll(indexMetaMap.get(type));
    }
    // meta:index_meta
    p.addColumn(
        Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
        Bytes.toBytes(META_TABLE_INDEX_META_QUALIFIER),
        SerializerUtils.serializeList(indexMetaList, IndexMeta.class));
    // meta:main_table_meta
    p.addColumn(
        Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
        Bytes.toBytes(META_TABLE_CORE_INDEX_META_QUALIFIER),
        SerializerUtils.serializeObject(dataSetMeta.getCoreIndexMeta()));
    // meta:desc
    p.addColumn(
        Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
        Bytes.toBytes(META_TABLE_DESC_QUALIFIER),
        Bytes.toBytes(dataSetMeta.getDesc()));
    if (dataSetMeta.getSetMeta() != null) {
      SetMeta setMeta = dataSetMeta.getSetMeta();
      // meta:start_time
      p.addColumn(
          Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
          Bytes.toBytes(start_time),
          SerializerUtils.serializeObject(setMeta.getStart_time()));
      // meta:srid
      p.addColumn(
          Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
          Bytes.toBytes(srid),
          SerializerUtils.serializeObject(setMeta.getSrid()));
      // meta:box
      p.addColumn(
          Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
          DATA_MBR,
          SerializerUtils.serializeObject(setMeta.getBoundingBox()));
      // meta:data_start_time
      p.addColumn(
          Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
          DATA_START_TIME,
          SerializerUtils.serializeObject(setMeta.getTimeLine().getTimeStart()));
      // meta:data_end_time
      p.addColumn(
          Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
          DATA_END_TIME,
          SerializerUtils.serializeObject(setMeta.getTimeLine().getTimeEnd()));
      // meta:data_count
      p.addColumn(
          Bytes.toBytes(META_TABLE_COLUMN_FAMILY),
          DATA_START_TIME,
          SerializerUtils.serializeObject(setMeta.getDataCount()));
    }
    return p;
  }

  /** Convert HBase Result object into DataSetMeta object. */
  private DataSetMeta fromResult(Result result) throws IOException {
    byte[] CF = Bytes.toBytes(META_TABLE_COLUMN_FAMILY);
    byte[] INDEX_METAS_CQ = Bytes.toBytes(META_TABLE_INDEX_META_QUALIFIER);
    byte[] MAIN_TABLE_CQ = Bytes.toBytes(META_TABLE_CORE_INDEX_META_QUALIFIER);
    String dataSetName = new String(result.getRow());
    Cell cell1 = result.getColumnLatestCell(CF, INDEX_METAS_CQ);
    Cell cell2 = result.getColumnLatestCell(CF, MAIN_TABLE_CQ);
    SetMeta setMetaFromResult = createSetMetaFromResult(result);
    List<IndexMeta> indexMetaList =
        SerializerUtils.deserializeList(CellUtil.cloneValue(cell1), IndexMeta.class);
    IndexMeta mainTableMeta =
        (IndexMeta) SerializerUtils.deserializeObject(CellUtil.cloneValue(cell2), IndexMeta.class);
    return new DataSetMeta(dataSetName, indexMetaList, mainTableMeta, setMetaFromResult);
  }

  private SetMeta createSetMetaFromResult(Result result) {
    byte[] CF = Bytes.toBytes(META_TABLE_COLUMN_FAMILY);
    byte[] START_TIME_CQ = Bytes.toBytes(start_time);
    byte[] SRID = Bytes.toBytes(srid);
    Cell cell_start = result.getColumnLatestCell(CF, START_TIME_CQ);
    Cell cell_srid = result.getColumnLatestCell(CF, SRID);
    Cell cell_st = result.getColumnLatestCell(CF, DATA_START_TIME);
    Cell cell_et = result.getColumnLatestCell(CF, DATA_END_TIME);
    Cell cell_count = result.getColumnLatestCell(CF, DATA_COUNT);
    Cell cell_mbr = result.getColumnLatestCell(CF, DATA_MBR);
    ZonedDateTime start_time =
        (ZonedDateTime) SerializerUtils.deserializeObject(CellUtil.cloneValue(cell_start), ZonedDateTime.class);
    int srid =
        (int) SerializerUtils.deserializeObject(CellUtil.cloneValue(cell_srid), Integer.class);
    ZonedDateTime data_start_time =
        (ZonedDateTime)
            SerializerUtils.deserializeObject(CellUtil.cloneValue(cell_st), ZonedDateTime.class);
    ZonedDateTime data_end_time =
        (ZonedDateTime)
            SerializerUtils.deserializeObject(CellUtil.cloneValue(cell_et), ZonedDateTime.class);
    int count =
        (int) SerializerUtils.deserializeObject(CellUtil.cloneValue(cell_count), Integer.class);
    MinimumBoundingBox box =
        (MinimumBoundingBox)
            SerializerUtils.deserializeObject(
                CellUtil.cloneValue(cell_mbr), MinimumBoundingBox.class);
    TimeLine timeLine = new TimeLine(data_start_time, data_end_time);
    return new SetMeta(start_time, srid, box, timeLine, count);
  }

  public void close() {
    try {
      dataSetMetaTable.close();
    } catch (IOException e) {
      logger.error("Failed to close meta table instance, exception log: {}", e.getMessage());
    }
  }
}
