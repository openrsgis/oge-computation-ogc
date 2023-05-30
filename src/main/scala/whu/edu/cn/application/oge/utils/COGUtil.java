package whu.edu.cn.application.oge.utils;

import lombok.extern.slf4j.Slf4j;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Vector;

@Slf4j
public class COGUtil {

    /**
     *  call the shell
     * @param builder the command
     * @return if success
     */
    public boolean callShell(ProcessBuilder builder){
        String command = String.join(" ", builder.command());
        try {
            log.info("调用脚本程序" + command);
            builder.redirectErrorStream(true);
            Process p = builder.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                log.info(line);
            }
            p.waitFor();
            log.info("脚本程序执行完毕");
            return true;
        } catch (Exception e){
            e.printStackTrace();
            log.error(command + " execute fail");
            return false;
        }
    }

    public String generateCOG(String inPath, String outputPath, String levels){
        boolean flag3 = callShell(new ProcessBuilder("cmd.exe", "/c","conda activate cog && python E:\\LaoK\\code_test\\COGUtil\\COGUtil_windows.py " +
                "--inPath=" + inPath + " --outputPath=" + outputPath + " --levels=" + levels));
        if(flag3){
            return outputPath;
        }else{
            return null;
        }
    }

    public boolean isCOG(String filePath){
        gdal.AllRegister();
        Dataset dataset = gdal.Open(filePath, gdalconstConstants.GA_ReadOnly);
        Vector metadata = dataset.GetMetadata_List();
        return false;
    }

    public static void main(String[] args){
        log.info("ssssssss");

        COGUtil cogUtil = new COGUtil();
        cogUtil.isCOG("E:\\LaoK\\data2\\APITest\\NDWI\\LC08_L1TP_123039_20181102_20181115_01_T1_B3.tif");
//        cogUtil.generateCOG("E:\\LaoK\\data2\\APITest\\GDAL_LC08_L1TP_122038_20180604_20180615_01_T1_B3.TIF",
//                "E:\\LaoK\\data2\\APITest\\COG_LC08_L1TP_122038_20180604_20180615_01_T1_B3.TIF", "4");
    }
}
