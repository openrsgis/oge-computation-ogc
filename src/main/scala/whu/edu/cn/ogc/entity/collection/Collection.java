package whu.edu.cn.ogc.entity.collection;


import whu.edu.cn.ogc.entity.spatial.Extent;

public class Collection {

    private String productId;
    private int size;
    private String description;
    private String crs;
    private String itemType;
    private Extent extent;

    public Collection(String productId, int size, String description, String crs, String itemType, Extent extent) {
        this.productId = productId;
        this.size = size;
        this.description = description;
        this.crs = crs;
        this.itemType = itemType;
        this.extent = extent;
    }

    public Extent getExtent() {
        return extent;
    }

    public void setExtent(Extent extent) {
        this.extent = extent;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

//    public List<List<Float>> getBbox() {
//        return bbox;
//    }
//
//    public void setBbox(List<List<Float>> bbox) {
//        this.bbox = bbox;
//    }

    public String getCrs() {
        return crs;
    }

    public void setCrs(String crs) {
        this.crs = crs;
    }

//    public List<List<String>> getInterval() {
//        return interval;
//    }
//
//    public void setInterval(List<List<String>> interval) {
//        this.interval = interval;
//    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }
}
