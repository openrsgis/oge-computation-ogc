package whu.edu.cn.trajectory.core.conf.store;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY
)
@JsonSubTypes({
        @JsonSubTypes.Type(
                value = HDFSStoreConfig.class,
                name = "hdfs"
        ),
        @JsonSubTypes.Type(
                value = StandaloneStoreConfig.class,
                name = "standalone"
        ),
        @JsonSubTypes.Type(
                value = HBaseStoreConfig.class,
                name = "hbase"
        ),
        @JsonSubTypes.Type(
                value = HiveStoreConfig.class,
                name = "hive"
        )
})
//, @JsonSubTypes.Type(
//    value = FileOutputConfig.class,
//    name = "file"
//), @JsonSubTypes.Type(
//    value = GeoMesaOutputConfig.class,
//    name = "geoMesa"
//)
public interface IStoreConfig extends Serializable {
    StoreTypeEnum getStoreType();
}
