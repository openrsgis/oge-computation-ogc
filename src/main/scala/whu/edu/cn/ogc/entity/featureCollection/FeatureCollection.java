package whu.edu.cn.ogc.entity.featureCollection;

import com.alibaba.fastjson.JSONArray;
import whu.edu.cn.ogc.entity.collection.Collection;
import whu.edu.cn.ogc.entity.spatial.Extent;

public class FeatureCollection extends Collection {
    private JSONArray featureCollection;

    public FeatureCollection(String productId, int size, String description, String crs, String itemType, Extent extent, JSONArray featureCollection) {
        super(productId, size, description, crs, itemType, extent);
        this.featureCollection = featureCollection;
    }

    public JSONArray getFeatureCollection() {
        return featureCollection;
    }

    public void setFeatureCollection(JSONArray featureCollection) {
        this.featureCollection = featureCollection;
    }
}
