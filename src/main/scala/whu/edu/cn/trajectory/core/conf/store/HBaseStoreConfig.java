package whu.edu.cn.trajectory.core.conf.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import scala.NotImplementedError;
import whu.edu.cn.trajectory.core.enums.StoreSchemaEnum;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HBaseStoreConfig implements IStoreConfig {
    @Override
    public StoreTypeEnum getStoreType() {
        return null;
    }

//    private final String location;
//    private final String dataSetName;
//    private final IndexType mainIndex;
//    private String otherIndex;
//    private final StoreSchemaEnum schema;
//    private final List<IndexMeta> indexList;
//    private final DataSetMeta dataSetMeta;
//
//    @JsonCreator
//    public HBaseStoreConfig(
//            @JsonProperty("location") String location,
//            @JsonProperty("dataSetName") String dataSetName,
//            @JsonProperty("schema") StoreSchemaEnum schema,
//            @JsonProperty("mainIndex") IndexType mainIndex,
//            @JsonProperty("otherIndex") @JsonInclude(JsonInclude.Include.NON_NULL) String otherIndex) {
//        this.location = location;
//        this.dataSetName = dataSetName;
//        this.schema = schema;
//        this.mainIndex = mainIndex;
//        this.otherIndex = otherIndex;
//        this.indexList = createIndexList();
//        this.dataSetMeta = new DataSetMeta(this.dataSetName, this.indexList);
//    }
//
//    @Override
//    public StoreTypeEnum getStoreType() {
//        return StoreTypeEnum.HBASE;
//    }
//
//    public String getLocation() {
//        return location;
//    }
//
//    public String getDataSetName() {
//        return dataSetName;
//    }
//
//    public IndexType getMainIndex() {
//        return mainIndex;
//    }
//
//    public String getOtherIndex() {
//        return otherIndex;
//    }
//
//    public StoreSchemaEnum getSchema() {
//        return schema;
//    }
//
//    public List<IndexMeta> getIndexList() {
//        return indexList;
//    }
//
//    public DataSetMeta getDataSetMeta() {
//        return dataSetMeta;
//    }
//
//    public void setOtherIndex(String otherIndex) {
//        this.otherIndex = otherIndex;
//    }
//
//    private List<IndexMeta> createIndexList() {
//        List<IndexMeta> indexMetaList = new LinkedList<>();
//        IndexMeta mainIndexMeta = createIndexMeta(mainIndex, true);
//        indexMetaList.add(mainIndexMeta);
//        if (otherIndex != null) {
//            List<IndexMeta> otherIndexMeta = createOtherIndex(otherIndex, TextSplitType.CSV);
//            indexMetaList.addAll(otherIndexMeta);
//        }
//        checkIndexMeta(indexMetaList, mainIndexMeta);
//        return indexMetaList;
//    }
//    private void checkIndexMeta(List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta){
//        // 检查重复
//        HashSet<IndexMeta> hashSet = new HashSet<>(indexMetaList);
//        if (hashSet.size() != indexMetaList.size()) {
//            throw new IllegalArgumentException("found duplicate index meta in the list.");
//        }
//    }
//
//    private IndexMeta createIndexMeta(IndexType indexType, Boolean isMainIndex) {
//        switch (indexType) {
//            case XZ2:
//                return new IndexMeta(isMainIndex, new XZ2IndexStrategy(), dataSetName, "default");
//            case XZ2Plus:
//                return new IndexMeta(isMainIndex, new XZ2PlusIndexStrategy(), dataSetName, "default");
//            case TXZ2:
//                return new IndexMeta(isMainIndex, new TXZ2IndexStrategy(), dataSetName, "default");
//            case XZ2T:
//                return new IndexMeta(isMainIndex, new XZ2TIndexStrategy(), dataSetName, "default");
//            case OBJECT_ID_T:
//                return new IndexMeta(isMainIndex, new IDTIndexStrategy(), dataSetName, "default");
//            default:
//                throw new NotImplementedError();
//        }
//    }
//
//    private List<IndexMeta> createOtherIndex(String otherIndex, TextSplitType splitType) {
//        String[] indexValue = otherIndex.split(splitType.getDelimiter());
//        ArrayList<IndexMeta> indexMetaList = new ArrayList<>();
//        for (String index : indexValue) {
//            IndexType indexType = IndexType.valueOf(index);
//            IndexMeta indexMeta = createIndexMeta(indexType, false);
//            indexMetaList.add(indexMeta);
//        }
//        return indexMetaList;
//    }
//
//    @Override
//    public String toString() {
//        return "HBaseStoreConfig{" +
//                "mainIndex=" + mainIndex +
//                ", otherIndex='" + otherIndex + '\'' +
//                ", dataSetMeta=" + dataSetMeta +
//                '}';
//    }
}
