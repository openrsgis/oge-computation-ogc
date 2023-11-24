package whu.edu.cn.trajectory.core.conf.store;

import java.io.Serializable;

public enum StoreTypeEnum implements Serializable {
    STANDALONE("standalone"),
    HDFS("hdfs"),
    HBASE("hbase"),
    HIVE("hive"),
    GEOMESA("geomesa");

    private String type;

    StoreTypeEnum(String type) {
        this.type = type;
    }

    public String toString() {
        return "OutputType{type='" + this.type + '\'' + '}';
    }

    static class Constants {
        static final String HDFS = "hdfs";
        static final String STANDALONE = "standalone";
        static final String HBASE = "hbase";
        static final String HIVE = "hive";
        static final String GEOMESA = "geomesa";
    }
}
