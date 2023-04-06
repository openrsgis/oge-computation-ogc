package whu.edu.cn.ogc.ogcAPIUtil;

import lombok.extern.slf4j.Slf4j;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class SpatialUtil {
    /**
     * 空间参考系标识符转换为EPSG组织维护的全球的空间参考标识符
     * @param crsUri OGC API传入的空间参考标识符
     * @return String EPSG:XXXX
     */
    public static String crsUri2Crs(String crsUri){
        if(crsUri.equals("http://www.opengis.net/def/crs/OGC/1.3/CRS84")){
            return "EPSG:4326";
        }else if(crsUri.startsWith("http://www.opengis.net/def/crs/EPSG")){
            // 如果是EPSG开头
            return "EPSG:" + crsUri.split("/")[crsUri.split("/").length -1];
        }
        else{
            return "EPSG:4326";
        }
    }

    /**
     * 给出两个空间范围，判断是否相交
     * @param bbox1 bbox1
     * @param bbox2 bbox2
     * @param bboxCrs1 bbox1的参考系
     * @param bboxCrsUri bbox2的参考系是URI形式
     * @return boolean 是否相交，相交返回true，否则返回false
     * @throws FactoryException 参考系异常
     * @throws TransformException 参考系转换异常
     */
    public static boolean isHavePublicArea(List<Float> bbox1, List<Float> bbox2, String bboxCrs1, String bboxCrsUri) throws FactoryException, TransformException {
        String bboxCrs2 = crsUri2Crs(bboxCrsUri);
        Envelope envelope1 = new  Envelope(bbox1.get(0), bbox1.get(2), bbox1.get(1), bbox1.get(3));
        Envelope envelope2 = new  Envelope(bbox2.get(0), bbox2.get(2), bbox2.get(1), bbox2.get(3));
        if(!bboxCrs1.equals(bboxCrs2)){
            CoordinateReferenceSystem sourceCRS = CRS.decode(bboxCrs1, true);
            CoordinateReferenceSystem targetCRS = CRS.decode(bboxCrs2, true);
            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
            envelope1 = JTS.transform(envelope1, transform);
        }
        return envelope1.equals(envelope2) || envelope1.intersects(envelope2) || envelope1.contains(envelope2) || envelope2.contains(envelope1);
    }

    /**
     * 将Bbox都转换到EPSG:4326 然后合并
     * @param bbox1 bbox1
     * @param bbox2 bbox2
     * @param bboxCrs1 bbox1 坐标系
     * @param bboxCrs2 bbox2 坐标系
     * @return List<Float> 合并后的bbox
     * @throws FactoryException
     * @throws TransformException
     */
    public List<Float> mergeBbox(List<Float> bbox1, List<Float> bbox2, String bboxCrs1, String bboxCrs2) throws FactoryException, TransformException {
        Envelope envelope1 = new Envelope();
        Envelope envelope2 = new Envelope();
        if(bbox1 != null){
            envelope1 = new Envelope(bbox1.get(0), bbox1.get(2), bbox1.get(1), bbox1.get(3));
            if(!bboxCrs1.equals("EPSG:4326")){
                envelope1 =  transformBboxCRS(envelope1, bboxCrs1, "EPSG:4326");
            }
        }
       if(bbox2 != null){
           envelope2 = new  Envelope(bbox2.get(0), bbox2.get(2), bbox2.get(1), bbox2.get(3));
           if(!bboxCrs2.equals("EPSG:4326")){
               envelope2 =  transformBboxCRS(envelope2, bboxCrs2, "EPSG:4326");
           }
       }
        if(bbox1 == null && bbox2 == null){
            return null;
        }else if(bbox1 != null && bbox2 != null){
            envelope1.expandToInclude(envelope2);
            List<Float> mergedBbox = new ArrayList<>();
            mergedBbox.add((float) envelope1.getMinX());
            mergedBbox.add((float) envelope1.getMinY());
            mergedBbox.add((float) envelope1.getMaxX());
            mergedBbox.add((float) envelope1.getMaxY());
            return  mergedBbox;
        }else{
            return bbox1 == null ? bbox2: bbox1;
        }
    }

    public Envelope transformBboxCRS(Envelope envelope, String bboxCrs, String targetCrs) throws TransformException, FactoryException {
        CoordinateReferenceSystem sourceCRS = CRS.decode(bboxCrs, true);
        CoordinateReferenceSystem targetCRS = CRS.decode(targetCrs, true);
        MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);
        return JTS.transform(envelope, transform);
    }

}
