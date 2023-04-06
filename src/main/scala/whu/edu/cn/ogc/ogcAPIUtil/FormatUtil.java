package whu.edu.cn.ogc.ogcAPIUtil;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import java.util.Base64;

@Slf4j
public class FormatUtil {

    public boolean isGeoJSON(String jsonStr) {
        try {
            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
            if (jsonObj.containsKey("type")) {
                String type = jsonObj.getString("type");
                if (type.equals("Feature")) {
                    return jsonObj.containsKey("geometry");
                } else if (type.equals("FeatureCollection")) {
                    return jsonObj.containsKey("features") && jsonObj.getJSONArray("features") != null;
                }
            }
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("判断GeoJSON出错");
            return false;
        }
    }

    public boolean isBase64(String str) {
        try {
            Base64.getDecoder().decode(str);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
