package whu.edu.cn.trajectory.core.operator.transform.source;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.base.util.BasicDateUtils;
import whu.edu.cn.trajectory.core.common.constant.TrajectoryDefaultConstant;
import whu.edu.cn.trajectory.core.conf.data.TrajectoryConfig;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/20
 */
public class KML2Traj {
    public static Trajectory parseKMLToTrajectory(String sourcePath, TrajectoryConfig trajectoryConfig){

        List<Trajectory> trajectories = parseKMLToTrajectoryList(sourcePath, trajectoryConfig);
        return trajectories.get(0);
    }
    public static List<Trajectory> parseKMLToTrajectoryList(String sourcePath, TrajectoryConfig trajectoryConfig){
        File kmlFile = new File(sourcePath);
        ArrayList<Trajectory> trajectories = new ArrayList<>();
        try {
            // 创建 DocumentBuilder 对象
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            // 加载 KML 文件
            Document document = builder.parse(kmlFile.getPath());

            // 解析 KML 文件
            NodeList placemarkList = document.getElementsByTagName("Placemark");
            for (int i = 0; i < placemarkList.getLength(); i++) {
                Node placemark = placemarkList.item(i);
                String oid = null;
                String tid = null;
                String timestamp = null;
                if (placemark.getNodeType() == Node.ELEMENT_NODE) {
                    Element placemarkElement = (Element) placemark;
                    Node extendedData = placemarkElement.getElementsByTagName("ExtendedData").item(0);
                    Element extendedDataElement = (Element) extendedData;
                    NodeList dataList = extendedDataElement.getElementsByTagName("Data");
                    for (int j = 0; j < dataList.getLength(); j++) {
                        Element dataElement = (Element) dataList.item(j);
                        String name = dataElement.getAttribute("name");
                        if(name.equals(trajectoryConfig.getObjectId().getSourceName())){
                            oid = dataElement.getElementsByTagName("value").item(0).getTextContent();
                        }else if(name.equals(trajectoryConfig.getTrajId().getSourceName())){
                            tid = dataElement.getElementsByTagName("value").item(0).getTextContent();
                        }else if (name.equals(trajectoryConfig.getTimeList().getSourceName())){
                            timestamp = dataElement.getElementsByTagName("value").item(0).getTextContent();
                        }
                    }
                    Node lineString = placemarkElement.getElementsByTagName("LineString").item(0);
                    Element lineStringElement = (Element) lineString;
                    Element coordinatesElement = (Element) lineStringElement.getElementsByTagName("coordinates").item(0);
                    String coordinateData = coordinatesElement.getTextContent();
                    String[] coordinates = coordinateData.split(" ");
                    List<TrajPoint> trajPoints = new ArrayList<>();
                    for (int index = 0 ; index < coordinates.length; index++) {
                        String coordinate = coordinates[index];
                        String[] latLng = coordinate.split(",");
                        double longitude = Double.parseDouble(latLng[0]);
                        double latitude = Double.parseDouble(latLng[1]);
                        if(timestamp != null){
                            List<Long> timestampList = createTimestampList(timestamp);
                            TrajPoint trajPoint =
                                    new TrajPoint(
                                            BasicDateUtils.timeToZonedTime(timestampList.get(index)), longitude, latitude);
                            trajPoints.add(trajPoint);
                        }else {
                            TrajPoint trajPoint =
                                    new TrajPoint(
                                            TrajectoryDefaultConstant.DEFAULT_DATETIME, longitude, latitude);
                            trajPoints.add(trajPoint);
                        }
                    }
                    trajectories.add(new Trajectory(tid, oid, trajPoints));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return trajectories;
    }
    public static List<Long> createTimestampList(String time){
        ArrayList<Long> list = new ArrayList<>();
        String substring = time.replaceAll("\\[|\\]", ""); // 去掉方括号
        String[] split = substring.split(",");
        for (String s : split) {
            list.add(Long.parseLong(s.trim()));
        }
        return list;
    }
}
