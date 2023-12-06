package whu.edu.cn.trajectory.db.coding.coding;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.db.coding.sfc.SFCRange;
import whu.edu.cn.trajectory.db.coding.sfc.XZ2SFC;
import whu.edu.cn.trajectory.db.constant.CodingConstants;
import whu.edu.cn.trajectory.db.datatypes.ByteArray;
import whu.edu.cn.trajectory.db.query.basic.condition.SpatialQueryCondition;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class XZ2Coding implements SpatialCoding {

    private static final Logger logger = LoggerFactory.getLogger(XZ2Coding.class);

    public static final int BYTES = Long.BYTES;

    private XZ2SFC xz2Sfc;

    short xz2Precision;

    public XZ2Coding() {
        xz2Precision = CodingConstants.MAX_XZ2_PRECISION;
        xz2Sfc = XZ2SFC.getInstance(xz2Precision);
    }

    public XZ2SFC getXz2Sfc() {
        return xz2Sfc;
    }

    /**
     * Get xz2 index for the line string.
     *
     * @param lineString Line string to be indexed.
     * @return The XZ2 code
     */
    public ByteArray code(LineString lineString) {
        Envelope boundingBox = lineString.getEnvelopeInternal();
        double minLng = boundingBox.getMinX();
        double maxLng = boundingBox.getMaxX();
        double minLat = boundingBox.getMinY();
        double maxLat = boundingBox.getMaxY();
        // lenient is false so the points out of boundary can throw exception.
        ByteBuffer br = ByteBuffer.allocate(Long.BYTES);
        br.putLong(xz2Sfc.index(minLng, maxLng, minLat, maxLat, false));
        return new ByteArray(br);
    }

    /**
     * Get index ranges of the query range, support two spatial query types
     * @param spatialQueryCondition Spatial query on the index.
     * @return List of xz2 index ranges corresponding to the query range.
     */
    public List<CodingRange> ranges(SpatialQueryCondition spatialQueryCondition) {
        Envelope envelope = spatialQueryCondition.getQueryWindow();
        List<CodingRange> codingRangeList = new LinkedList<>();
        List<SFCRange> sfcRangeList = xz2Sfc.ranges(envelope, spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN);
        for (SFCRange sfcRange : sfcRangeList) {
            CodingRange codingRange = new CodingRange();
            codingRange.concatSfcRange(sfcRange);
            codingRangeList.add(codingRange);
        }
        return codingRangeList;
    }

    @Override
    public Polygon getCodingPolygon(ByteArray spatialCodingByteArray) {
        ByteBuffer br = spatialCodingByteArray.toByteBuffer();
        ((Buffer) br).flip();
        long coding = br.getLong();
        return xz2Sfc.getEnlargedRegion(coding);
    }

    @Override
    public String toString() {
        return "XZ2Coding{" +
                "xz2Precision=" + xz2Precision +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XZ2Coding xz2Coding = (XZ2Coding) o;
        return xz2Precision == xz2Coding.xz2Precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xz2Precision);
    }
}
