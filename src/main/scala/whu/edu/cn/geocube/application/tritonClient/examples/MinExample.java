package whu.edu.cn.geocube.application.tritonClient.examples;

import java.util.List;

import com.google.common.collect.Lists;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import whu.edu.cn.geocube.application.tritonClient.*;
import whu.edu.cn.geocube.application.tritonClient.pojo.DataType;

import static org.gdal.gdalconst.gdalconstConstants.*;

public class MinExample {
    public static void main(String[] args) throws Exception {

        boolean isBinary = true;
        InferInput inputIds = new InferInput("input0", new long[]{1L, 3L, 512L, 512L}, DataType.FP32);
        //float[] inputIdsData = new float[786432];
        float[] inputIdsData = readPNG("D:/000011.png");
        inputIds.setData(inputIdsData, isBinary);

        List<InferInput> inputs = Lists.newArrayList(inputIds);
        List<InferRequestedOutput> outputs = Lists.newArrayList(new InferRequestedOutput("output0", isBinary));

        InferenceServerClient client = new InferenceServerClient("125.220.153.57:8000", 5000, 5000);
        InferResult result = client.infer("crop_recog_swin_onnx", inputs, outputs);
        float[] logits = result.getOutputAsFloat("output0");
        //System.out.println(Arrays.toString(logits));
        writePNG(logits, "D:/a.png");
    }

    public static float[] readPNG(String path) {
        String dstPath = path;
        gdal.AllRegister();
        Dataset srcDataset = gdal.Open(path, gdalconstConstants.GA_ReadOnly);
        int[] tValue1 = new int[512 * 512];
        int[] tValue2 = new int[512 * 512];
        int[] tValue3 = new int[512 * 512];
        float[] input = new float[3 * 512 * 512];
        srcDataset.GetRasterBand(1).ReadRaster(0, 0, 512, 512, tValue1);
        srcDataset.GetRasterBand(2).ReadRaster(0, 0, 512, 512, tValue2);
        srcDataset.GetRasterBand(3).ReadRaster(0, 0, 512, 512, tValue3);
        for (int i = 0; i < 512 * 512; i++) {
            input[i] = (float) (tValue1[i] / 255.0);
            input[i + 512 * 512] = (float) (tValue2[i] / 255.0);
            input[i + 2 * 512 * 512] = (float) (tValue3[i] / 255.0);
        }
        return input;
    }

    public static void writePNG(float[] output, String path) {
        gdal.AllRegister();
        Driver dr = gdal.GetDriverByName("PNG");
        Driver dr1 = gdal.GetDriverByName("MEM");
        Dataset dm = dr1.Create(path, 512, 512, 1, GDT_Byte);

        byte[] band = new byte[512 * 512];
        for (int i = 0; i < 512 * 512; i++) {
            if (output[i] > output[i + 512 * 512]) {
                band[i] = 0;
            } else {
                band[i] = -1;
            }
        }
        for (int i = 0; i < 512 * 512; i++) {
            if (band[i] == 127) {
                System.out.println("i = " + i);
            }
        }

        dm.GetRasterBand(1).WriteRaster(0, 0, 512, 512, 512, 512, GDT_Byte, band);
        dr.CreateCopy(path, dm);
    }
}