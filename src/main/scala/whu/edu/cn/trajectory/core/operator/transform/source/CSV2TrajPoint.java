package whu.edu.cn.trajectory.core.operator.transform.source;

import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.util.BasicDateUtils;
import whu.edu.cn.trajectory.core.conf.data.Mapping;
import whu.edu.cn.trajectory.core.conf.data.TrajPointConfig;
import whu.edu.cn.trajectory.core.util.DataTypeUtils;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/11/15
 */
public class CSV2TrajPoint {

    public static TrajPoint parse(String rawString, TrajPointConfig config, String splitter) throws
            IOException {
        try {
            String[] record = rawString.split(splitter);
            int pidIdx = config.getPointId().getIndex();
            String id;
            if (pidIdx < 0) {
                id = null;
            } else {
                id = record[config.getPointId().getIndex()];
            }
            ZonedDateTime
                    time = BasicDateUtils.parse(config.getTime().getBasicDataTypeEnum(),
                    record[config.getTime().getIndex()], config.getTime());
            double lat = Double.parseDouble(record[config.getLat().getIndex()]);
            double lng = Double.parseDouble(record[config.getLng().getIndex()]);
            int n = record.length;
            Map<String, Object> metas = new HashMap<>(n);
            if (config.getTrajPointMetas() != null) {
                // 对于明确指定映射名的列，赋予映射名，其他列顺序赋映射名
                List<Mapping> trajPointMetas = config.getTrajPointMetas();
                int j = 0;
                for (int i = 0; i < n; ++i) {
                    if (j < trajPointMetas.size() && i == trajPointMetas.get(j).getIndex()) {
                        Mapping m = trajPointMetas.get(j);
                        metas.put(m.getMappingName(),
                                DataTypeUtils.parse(record[m.getIndex()], m.getDataType(), m.getSourceData()));
                        j++;
                    } else {
                        metas.put(String.valueOf(i), record[i]);
                    }
                }
            } else {
                // 无明确指定，则顺序生成映射名
                for (int i = 0; i < n; ++i) {
                    metas.put(String.valueOf(i), record[i]);
                }
            }
            return new TrajPoint(id, time, lng, lat, metas);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

}
