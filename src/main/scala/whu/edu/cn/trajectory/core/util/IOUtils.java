package whu.edu.cn.trajectory.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/15
 */
public class IOUtils {

  public static List<String> getFileNames(String fatherPath) {
    List<String> files = new ArrayList<>();
    File fatherFile = new File(fatherPath);
    File[] fileList = fatherFile.listFiles();

    for (int i = 0; i < (fileList != null ? fileList.length : 0); ++i) {
      if (fileList[i].isFile()) {
        files.add(fileList[i].toString());
      }
    }
    return files;
  }

  /**
   * 按行读取CSV文件
   *
   * @param filePath 文件路径（包括文件名）
   * @param hasTitle 是否有标题行
   * @return
   */
  public static List<String> readCSV(String filePath, boolean hasTitle) {
    List<String> data = new ArrayList<>();
    String line = null;
    try {
      BufferedReader bufferedReader =
          new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "utf-8"));
      if (hasTitle) {
        // 第一行信息，为标题信息，不返回
        line = bufferedReader.readLine();
        //        data.add(line);
        System.out.println("标题行：" + line);
      }
      while ((line = bufferedReader.readLine()) != null) {
        // 数据行
        data.add(line);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return data;
  }

  public static String readLocalTextFile(String path) {
    File file = new File(path);
    StringBuilder sb = new StringBuilder();
    try {
      Reader reader = new InputStreamReader(Files.newInputStream(file.toPath()));
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        sb.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return sb.toString();
  }

  public static String readLocalTextFileLine(String path) {
    File file = new File(path);
    StringBuilder sb = new StringBuilder();
    try {
      Reader reader = new InputStreamReader(Files.newInputStream(file.toPath()));
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        sb.append(line).append("\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return sb.toString();
  }

  public static void writeStringToFile(String fileName, String content) throws IOException {
    try {
      File file = new File(fileName);
      if (!file.exists()) {
        file.getParentFile().mkdirs();
        file.createNewFile();
      }
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(content);
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeStringToHdfsFile(String fileName, String content) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path filePath = new Path(fileName);

    // 如果文件已经存在则删除
    if (fs.exists(filePath)) {
      fs.delete(filePath, true);
    }

    // 将数据写入HDFS中
    try (InputStream in = new ByteArrayInputStream(content.getBytes())) {
      try (OutputStream out = fs.create(filePath)) {
        org.apache.hadoop.io.IOUtils.copyBytes(in, out, conf);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
