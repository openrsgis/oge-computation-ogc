package whu.edu.cn.ogc.entity.feature;

import com.alibaba.fastjson.JSONObject;
import whu.edu.cn.ogc.entity.item.Item;


import java.util.List;

public class Feature extends Item {

    private JSONObject feature;

    public Feature(String id, List<String> crs, String dateTime, String itemType, JSONObject feature) {
        super(id, crs, dateTime, itemType);
        this.feature = feature;
    }

    public JSONObject getFeature() {
        return feature;
    }

    public void setFeature(JSONObject feature) {
        this.feature = feature;
    }
}
