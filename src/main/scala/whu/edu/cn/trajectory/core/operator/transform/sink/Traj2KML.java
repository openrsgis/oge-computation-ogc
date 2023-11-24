package whu.edu.cn.trajectory.core.operator.transform.sink;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.locationtech.jts.geom.Coordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.base.trajectory.TrajFeatures;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/17
 */
public class Traj2KML {
  private static final Logger logger = LoggerFactory.getLogger(Traj2KML.class);

  public static void convertTrajListToKML(String outputPath, List<Trajectory> trajectoryList)
      throws IOException {
    File file = new File(outputPath);
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }
    // 创建KML文档
    Element root = DocumentHelper.createElement("kml");
    Document document = DocumentHelper.createDocument(root);
    // 根节点添加属性
    root.addAttribute("xmlns", "http://www.opengis.net/kml/2.2")
        .addAttribute("xmlns:gx", "http://www.google.com/kml/ext/2.2")
        .addAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        .addAttribute(
            "xsi:schemaLocation",
            "http://www.opengis.net/kml/2.2 http://schemas.opengis.net/kml/2.2.0/ogckml22.xsd http://www.google.com/kml/ext/2.2 http://code.google.com/apis/kml/schema/kml22gx.xsd");
    // 创建子元素 <Document>
    Element documentElement = root.addElement("Document");
    for (Trajectory trajectory : trajectoryList) {
      createDocument(documentElement, trajectory);
    }
    writeKML(outputPath, document);
  }

  public static void convertTrajToWKT(String outputPath, Trajectory trajectory) throws IOException {
    File file = new File(outputPath);
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }
    // 创建KML文档
    Element root = DocumentHelper.createElement("kml");
    Document document = DocumentHelper.createDocument(root);
    // 根节点添加属性
    root.addAttribute("xmlns", "http://www.opengis.net/kml/2.2")
            .addAttribute("xmlns:gx", "http://www.google.com/kml/ext/2.2")
            .addAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
            .addAttribute(
                    "xsi:schemaLocation",
                    "http://www.opengis.net/kml/2.2 http://schemas.opengis.net/kml/2.2.0/ogckml22.xsd http://www.google.com/kml/ext/2.2 http://code.google.com/apis/kml/schema/kml22gx.xsd");
    // 创建子元素 <Document>
    Element documentElement = root.addElement("Document");
    createDocument(documentElement, trajectory);
    writeKML(outputPath, document);
  }
  public static void createDocument(Element documentElement, Trajectory trajectory){
    // 创建子元素 <Placemark>
    Element placemarkElement = documentElement.addElement("Placemark");
    // 创建子元素 <ExtendedData>
    Element extendedDataElement = placemarkElement.addElement("ExtendedData");

    if(!trajectory.isUpdateFeatures()){
      TrajFeatures trajectoryFeatures = trajectory.getTrajectoryFeatures();
      // 添加 <Data> 元素
      addDataElement(extendedDataElement,"startTime", trajectoryFeatures.getStartTime().toString());
      addDataElement(extendedDataElement,"endTime", trajectoryFeatures.getEndTime().toString());
      addDataElement(extendedDataElement, "startPoint", trajectoryFeatures.getStartPoint().getPointSequence());
      addDataElement(extendedDataElement, "endPoint", trajectoryFeatures.getEndPoint().getPointSequence());
      addDataElement(extendedDataElement, "pointNum", String.valueOf(trajectoryFeatures.getPointNum()));
      addDataElement(extendedDataElement, "speed", String.valueOf(trajectoryFeatures.getSpeed()));
      addDataElement(extendedDataElement, "len", String.valueOf(trajectoryFeatures.getLen()));
    }
    addDataElement(extendedDataElement, "oid", trajectory.getObjectID());
    addDataElement(extendedDataElement, "tid", trajectory.getTrajectoryID());
    addDataElement(extendedDataElement, "timestamp", trajectory.getLineString().getUserData().toString());
    Element lineStringElement = placemarkElement.addElement("LineString");
    Element coordinatesElement = lineStringElement.addElement("coordinates");
    // 生成KML格式的坐标字符串
    StringBuilder coordinateBuilder = new StringBuilder();
    for (Coordinate coordinate : trajectory.getLineString().getCoordinates()) {
      coordinateBuilder.append(coordinate.x).append(",").append(coordinate.y).append(" ");
    }
    coordinatesElement.setText(coordinateBuilder.toString().trim());
  }

  // 添加 <Data> 元素的方法
  private static void addDataElement(Element parentElement, String name, String value) {
    Element dataElement = parentElement.addElement("Data");
    dataElement.addAttribute("name", name);
    Element valueElement = dataElement.addElement("value");
    valueElement.setText(value);
  }
  private static void writeKML(String outPath, Document document) throws IOException {
    //创建kml到本地
    OutputFormat format = OutputFormat.createPrettyPrint();
    format.setEncoding("utf-8");
    XMLWriter xmlWriter = new XMLWriter(Files.newOutputStream(Paths.get(outPath)),format);
    xmlWriter.write(document);
    xmlWriter.close();
  }
}
