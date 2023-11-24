package whu.edu.cn.trajectory.core.conf.load;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME
)
@JsonSubTypes({
        @JsonSubTypes.Type(
                value = HDFSLoadConfig.class,
                name = "hdfs"
        ),
        @JsonSubTypes.Type(
                value = StandaloneLoadConfig.class,
                name = "standalone"
        ),
        @JsonSubTypes.Type(
                value = HBaseLoadConfig.class,
                name = "hbase"
        ),
        @JsonSubTypes.Type(
                value = HiveLoadConfig.class,
                name = "hive"
        )
})

public interface ILoadConfig extends Serializable {
    InputType getInputType();

    String getFsDefaultName();

    enum InputType implements Serializable {
        STANDALONE("standalone"),
        HDFS("hdfs"),
        HBASE("hbase"),
        HIVE("hive"),
        GEOMESA("geomesa");

        private String inputType;

        InputType(String inputType) {
            this.inputType = inputType;
        }

        public final String toString() {
            return "InputType{type='" + this.inputType + '\'' + '}';
        }
    }
}
