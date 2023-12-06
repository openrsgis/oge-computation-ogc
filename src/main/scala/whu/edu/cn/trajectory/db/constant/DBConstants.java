package whu.edu.cn.trajectory.db.constant;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.ZoneId;
import java.time.ZoneOffset;

import static whu.edu.cn.trajectory.base.trajectory.Trajectory.Schema.*;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class DBConstants {
    // Tables
    public static final String META_TABLE_NAME = "trajspark_db_meta";
    public static final String META_TABLE_COLUMN_FAMILY = "meta";
    public static final String META_TABLE_INDEX_META_QUALIFIER = "index_meta";
    public static final String META_TABLE_CORE_INDEX_META_QUALIFIER = "main_table_index_meta";
    public static final String META_TABLE_DESC_QUALIFIER = "desc";

    // INDEX TABLE COLUMNS
    public static final String DATA_TABLE_SUFFIX = "_data";
    public static String INDEX_TABLE_CF = "cf0";
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes(INDEX_TABLE_CF);
    public static final byte[] TRAJECTORY_ID_QUALIFIER = Bytes.toBytes(TRAJECTORY_ID);
    public static final byte[] OBJECT_ID_QUALIFIER = Bytes.toBytes(OBJECT_ID);
    public static final byte[] MBR_QUALIFIER = Bytes.toBytes(MBR);
    public static final byte[] START_POINT_QUALIFIER = Bytes.toBytes(START_POSITION);
    public static final byte[] END_POINT_QUALIFIER = Bytes.toBytes(END_POSITION);
    public static final byte[] START_TIME_QUALIFIER = Bytes.toBytes(START_TIME);
    public static final byte[] END_TIME_QUALIFIER = Bytes.toBytes(END_TIME);
    public static final byte[] TRAJ_POINTS_QUALIFIER = Bytes.toBytes(TRAJ_POINTS);
    public static final byte[] PTR_QUALIFIER = Bytes.toBytes(PTR);
    public static final byte[] POINT_NUMBER_QUALIFIER = Bytes.toBytes(POINT_NUMBER);
    public static final byte[] SPEED_QUALIFIER = Bytes.toBytes(SPEED);
    public static final byte[] LENGTH_QUALIFIER = Bytes.toBytes(LENGTH);
    public static final byte[] EXT_VALUES_QUALIFIER = Bytes.toBytes(EXT_VALUES);

    // Bulk load
    public static final String BULK_LOAD_TEMP_FILE_PATH_KEY = "import.file.output.path";
    public static final String BULK_LOAD_INPUT_FILE_PATH_KEY = "import.process.input.path";
    public static final String BULKLOAD_TARGET_INDEX_NAME = "bulkload.target.index.name";
    public static final String BULKLOAD_TEXT_PARSER_CLASS = "bulkload.parser.class";
    public static final String ENABLE_SIMPLE_SECONDARY_INDEX = "enable.simple.secondary.index";

    // Connection
    public static final String OPEN_CONNECTION_FAILED = "Cannot connect to data base.";
    public static final String CLOSE_CONNECTION_FAILED = "Close connection failed.";

    // Initial
    public static final String INITIAL_FAILED = "Initial failed.";

}
