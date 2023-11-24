package whu.edu.cn.trajectory.core.conf.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import whu.edu.cn.trajectory.core.enums.FileTypeEnum;
import whu.edu.cn.trajectory.core.enums.StoreSchemaEnum;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class StandaloneStoreConfig implements IStoreConfig {

  private String location;
  private StoreSchemaEnum schema;

  private String splitter;
  private String filePostFix;
  private FileTypeEnum fileType;

  @JsonCreator
  public StandaloneStoreConfig(
      @JsonProperty("location") String location,
      @JsonProperty("schema") StoreSchemaEnum schema,
      @JsonProperty("splitter") @JsonInclude(JsonInclude.Include.NON_NULL) String splitter,
      @JsonProperty("fileType") FileTypeEnum fileType) {
    this.location = location;
    this.schema = schema;
    this.splitter = splitter;
    this.filePostFix = "." + fileType.getFileTypeEnum();
    this.fileType = fileType;
  }

  public String getFilePostFix() {
    return filePostFix;
  }

  public String getSplitter() {
    return splitter;
  }

  public FileTypeEnum getFileType() {
    return fileType;
  }

  public StoreTypeEnum getStoreType() {
    return StoreTypeEnum.STANDALONE;
  }

  public String getLocation() {
    return this.location;
  }

  public StoreSchemaEnum getSchema() {
    return this.schema;
  }
}
