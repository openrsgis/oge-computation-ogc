package whu.edu.cn.util;

public class SystemConstants {
    public static final String DAG_ROOT_URL = "http://localhost:8085/oge-dag"; // dag-boot 服务根路径
    public static final String JEDIS_HOST = "192.168.226.147";
    public static final int JEDIS_PORT = 6379;

    public static final String MINIO_URL = "http://125.220.153.22:9006";
    public static final String MINIO_KEY = "rssample";
    public static final String MINIO_PWD = "ypfamily608";

    public static final String WORK_PREFIX = "oge:computation_ogc:existedTiles:";
    public static final int HEAD_SIZE = 262143;

    private byte[] header = new byte[HEAD_SIZE];

}
