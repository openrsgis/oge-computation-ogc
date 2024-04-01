package whu.edu.cn.algorithms.gmrc.colorbalanceRef.java;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;


public class CommonTools {

    public static void WriteTxtFile(String sFilePath, double[] params)
    {
        File file = new File(sFilePath);
        try
        {
            Writer out = new FileWriter(file);
            for (int i=0; i<params.length; i++)
            {
                out.write(String.format("%.6f",params[i]));
                out.write("\n");
            }
            out.flush();
            out.close();
        } catch (IOException e)
        {
            System.out.println(e.toString());

        }
    }

    public static boolean isNumericZidai(String str) {
        for (int i = 0; i < str.length(); i++) {
            System.out.println(str.charAt(i));
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean  MatchImgsHist(double[] input_cdf, double[] target_cdf, int[] matchMap, int MAX_LEVEL)
    {
        int[] input_eq = new int[MAX_LEVEL * 3];
        int[] target_eq = new int[MAX_LEVEL * 3];
        for (int i = 0; i < MAX_LEVEL; i++)
        {
            input_eq[i] = (int)Math.round((MAX_LEVEL - 1) * input_cdf[i]);
            input_eq[i + MAX_LEVEL] = (int)Math.round((MAX_LEVEL - 1) * input_cdf[i + MAX_LEVEL]);
            input_eq[i + MAX_LEVEL * 2] = (int)Math.round((MAX_LEVEL - 1) * input_cdf[i + MAX_LEVEL * 2]);
            target_eq[i] = (int)Math.round((MAX_LEVEL - 1) * target_cdf[i]);
            target_eq[i + MAX_LEVEL] = (int)Math.round((MAX_LEVEL - 1) * target_cdf[i + MAX_LEVEL]);
            target_eq[i + MAX_LEVEL * 2] = (int)Math.round((MAX_LEVEL - 1) * target_cdf[i + MAX_LEVEL * 2]);
        }

        for (int r = 0; r < MAX_LEVEL; r++)
        {
            for (int z = 0; z < MAX_LEVEL; z++)
            {
                if (input_eq[r] == target_eq[z])
                {
                    matchMap[r] = z;
                    break;
                }
                else if (input_eq[r] < target_eq[z])
                {
                    if (z == 0)
                        matchMap[r] = 0;
                    else if (z - input_eq[r] <= input_eq[r] - (z - 1))
                        matchMap[r] = z;
                    else
                        matchMap[r] = z - 1;
                    break;
                }
            }
        }
        for (int r = 0; r < MAX_LEVEL; r++)
        {
            for (int z = 0; z < MAX_LEVEL; z++)
            {
                if (input_eq[r + MAX_LEVEL] == target_eq[z + MAX_LEVEL])
                {
                    matchMap[r + MAX_LEVEL] = z;
                    break;
                }
                else if (input_eq[r + MAX_LEVEL] < target_eq[z + MAX_LEVEL])
                {
                    if (z == 0)
                        matchMap[r + MAX_LEVEL] = 0;
                    else if (z - input_eq[r + MAX_LEVEL] <= input_eq[r + MAX_LEVEL] - (z - 1))
                        matchMap[r + MAX_LEVEL] = z;
                    else
                        matchMap[r + MAX_LEVEL] = z - 1;
                    break;
                }
            }
        }

        for (int r = 0; r < MAX_LEVEL; r++)
        {
            for (int z = 0; z < MAX_LEVEL; z++)
            {
                if (input_eq[r + MAX_LEVEL * 2] == target_eq[z + MAX_LEVEL * 2])
                {
                    matchMap[r + MAX_LEVEL * 2] = z;
                    break;
                }
                else if (input_eq[r + MAX_LEVEL * 2] < target_eq[z + MAX_LEVEL * 2])
                {
                    if (z == 0)
                        matchMap[r + MAX_LEVEL * 2] = 0;
                    else if (z - input_eq[r + MAX_LEVEL * 2] <= input_eq[r + MAX_LEVEL * 2] - (z - 1))
                        matchMap[r + MAX_LEVEL * 2] = z;
                    else
                        matchMap[r + MAX_LEVEL * 2] = z - 1;
                    break;
                }
            }
        }
        return true;
    }


    public static void WriteTxtFile(String sFilePath, int[] params)
    {
        File file = new File(sFilePath);
        try
        {
            Writer out = new FileWriter(file);
            for (int i=0; i<params.length; i++)
            {
                out.write(String.format("%d\n", params[i]));
            }
            out.flush();
            out.close();
        } catch (IOException e)
        {
            System.out.println(e.toString());

        }
    }

    public static boolean IsFileExists(String sFilePath)
    {
        File file = new File(sFilePath);
        if (!file.exists()) return  false;
        return true;
    }

    public static String GetDirectory(String sFilePath)
    {
        File file = new File(sFilePath);
        return file.getParent();
    }
    public static String GetNameWithoutExt(String sFilePath)
    {
        File file = new File(sFilePath);
        String sName = file.getName();
        return sName.substring(0, sName.lastIndexOf("."));
        //return  sName.substring(sName.lastIndexOf("."));
    }
    public static String GetExt(String sFilePath)
    {
        File file = new File(sFilePath);
        String sName = file.getName();
        return  sName.substring(sName.lastIndexOf("."));
    }

    public static String GetImgDriverFormat(String sExt)
    {
        String str = null;
        switch (sExt)
        {
            case "":
                str = "MEM";
                break;
            case ".TIF":
            case ".TIFF":
                str = "GTiff";
                break;
            case ".JPG":
                str = "JPEG";
                break;
            case ".PNG":
                str = "PNG";
                break;
            default:
                str = "MEM";
                break;
        }
        return str;
    }

    public static void ClearBuf(int[] array, int value)
    {
        for (int i=0; i<array.length; i++)
        {
            array[i] = value;
        }
    }
    public static void ClearBuf(short[] array, short value)
    {
        for (int i=0; i<array.length; i++)
        {
            array[i] = value;
        }
    }
    public static void ClearBuf(boolean[] array, boolean value)
    {
        for (int i=0; i<array.length; i++)
        {
            array[i] = value;
        }
    }


    /**
     * 根据原始影像和参考影像分辨率，计算金字塔层级，使得原始影像顶层金字塔分辨率接近参考影像
     * @param dOriXres：原始影像x方向分辨率
     * @param dOriYres：原始影像y方向分辨率
     * @param dRefXres：参考影像x方向分辨率
     * @param dRefYres：参考影像y方向分辨率
     */
    public static double CalculatePyramidLevel(double dOriXres, double dOriYres, double dRefXres, double dRefYres)
    {
        double dRatioX = dRefXres / dOriXres;
        double dRatioY = dRefYres / dOriYres;
        double dRatio = Math.min(dRatioX, dRatioY);
        double dLevel = Math.log(dRatio) / Math.log(2.0);
        if (dLevel < 0.0)
        {
            dLevel = 0.0;
        }
        return dLevel;
    }

    /**
     * 根据影像像素最大值，计算影像超出八位的部分
     * @param maxValue：像素最大值
     * @return
     */
    public static int GetOffsetBit(double maxValue)
    {
        int n = (int)(Math.log(1.0*maxValue) / Math.log(2.0));
        int realSits = n - 8 + 1;
        return realSits ;
    }

    /**
     * 根据待处理影像和参考影像范围判断参考影像是否完全包含待处理影像
     * @param envInput:待处理影像外接矩形[xMin, yMin, xMax, yMax]
     * @param envRef：参考影像外界矩形
     * @return：是否完全包含
     */
    public static boolean IsWithIn(double[] envInput, double[] envRef)
    {
        if (envRef[0] <= envInput[0] && envRef[1] <= envInput[1] && envRef[2] >= envInput[2] &&envRef[3] >= envInput[3])
        {
            return true;
        }
        return false;
    }
}
