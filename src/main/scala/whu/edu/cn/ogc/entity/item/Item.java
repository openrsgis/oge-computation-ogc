package whu.edu.cn.ogc.entity.item;

import java.util.List;

public class Item {
    private String id;
    private List<String> crs;
    /**
     * the timeStamp of the feature
     */
    private String timeStamp;
    private String itemType;

    public Item(String id, List<String> crs, String timeStamp, String itemType) {
        this.id = id;
        this.crs = crs;
        this.timeStamp = timeStamp;
        this.itemType = itemType;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public List<String> getCrs() {
        return crs;
    }

    public void setCrs(List<String> crs) {
        this.crs = crs;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }
}
