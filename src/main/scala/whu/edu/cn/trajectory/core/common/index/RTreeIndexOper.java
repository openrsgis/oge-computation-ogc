package whu.edu.cn.trajectory.core.common.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class RTreeIndexOper implements Serializable {
    private final STRtree strtree = new STRtree();

    public RTreeIndexOper(List<Geometry> geometries) {
        geometries.forEach((g) -> {
            Envelope bbox = g.getEnvelopeInternal();
            this.strtree.insert(bbox, g);
        });
    }
    public RTreeIndexOper() {
    }
    public void add(Geometry geom) {
        if (geom != null) {
            Envelope bbox = geom.getEnvelopeInternal();
            this.strtree.insert(bbox, geom);
        }
    }

    public void buildIndex() {
        this.strtree.build();
    }

    public List<Geometry> searchIntersect(Geometry g, boolean parallel) {
        if (g == null) {
            return null;
        } else {
            Envelope bbox = g.getEnvelopeInternal();
            List<Geometry> candidates = this.strtree.query(bbox);
            return parallel ? (List) candidates.parallelStream().filter((o) -> {
                return !g.equals(o) && g.intersects(o);
            }).collect(Collectors.toList()) : (List) candidates.stream().filter((o) -> {
                return !g.equals(o) && g.intersects(o);
            }).collect(Collectors.toList());
        }
    }
}
