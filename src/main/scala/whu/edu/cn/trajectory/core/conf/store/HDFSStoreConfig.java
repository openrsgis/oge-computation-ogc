package whu.edu.cn.trajectory.core.conf.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import whu.edu.cn.trajectory.core.enums.FileTypeEnum;
import whu.edu.cn.trajectory.core.enums.StoreSchemaEnum;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HDFSStoreConfig implements IStoreConfig {
  private final String splitter;
  private final String filePostFix;
  private String ip;
  private int port;
  private String location;

  public String getSplitter() {
    return splitter;
  }

  public String getFilePostFix() {
    return filePostFix;
  }

  private StoreSchemaEnum schema;
  private FileTypeEnum fileType;

  @JsonCreator
  public HDFSStoreConfig(
      @JsonProperty("ip") String ip,
      @JsonProperty("port") int port,
      @JsonProperty("location") String location,
      @JsonProperty("schema") StoreSchemaEnum schema,
      @JsonProperty("splitter") String splitter,
      @JsonProperty("fileType") FileTypeEnum fileType) {
    this.ip = ip;
    this.port = port;
    this.location = location;
    this.schema = schema;
    this.splitter = splitter;
    this.filePostFix = "." + fileType.getFileTypeEnum();
    this.fileType = fileType;
  }

  public StoreTypeEnum getStoreType() {
    return StoreTypeEnum.HDFS;
  }

  public FileTypeEnum getFileType() {
    return fileType;
  }

  public String getIp() {
    return this.ip;
  }

  public int getPort() {
    return this.port;
  }

  public String getLocation() {
    return this.location;
  }

  public StoreSchemaEnum getSchema() {
    return this.schema;
  }
}
