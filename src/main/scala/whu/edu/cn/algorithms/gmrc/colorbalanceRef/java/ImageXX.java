package whu.edu.cn.algorithms.gmrc.colorbalanceRef.java;

import org.gdal.gdal.*;
import org.gdal.gdalconst.gdalconstConstants;

import java.util.Vector;

public class ImageXX {


    private static long MEMSIZE__ = 500000000;
    private static int[] mTempBuf = new int[(int)(MEMSIZE__/8)];

    private Dataset mDataset = null;
    //波段数
    private int m_nChannels = 0;
    //获取波段数
    public int getM_nChannels() {
        return m_nChannels;
    }
    //获取影像高度
    public int getM_nHeight() {
        return m_nHeight;
    }
    //获取影像宽度
    public int getM_nWidth() {
        return m_nWidth;
    }
    //获取影像位数
    public int getM_nBitSize() {
        return m_nBitSize;
    }
    //获取x方向分辨率（正数）
    public double getM_dXRes() {
        return m_dXRes;
    }
    //获取y方向分辨率（正数）
    public double getM_dYRes() {
        return m_dYRes;
    }

    //高度
    private int m_nHeight, m_nWidth;
    //宽度
    private int m_nBitSize = 0;
    //影像波段数组
    private int[] pBand = null;
    //变换参数
    private double[] mGeoTransform = null;
    //分辨率
    private double m_dXRes, m_dYRes;
    //外接矩形
    private double[] mEnv = null;

    //获取影像像素最小值
    public int getM_nMinValue() {
        return m_nMinValue;
    }
    //获取影像像素最大值
    public int getM_nMaxValue() {
        return m_nMaxValue;
    }

    public Boolean ClipWithShp(ImageXX destImg, String sShpFile)
    {
        Vector option = new Vector();
        //镶嵌线文件
        option.add("-cutline");
        option.add(sShpFile);
        //设置sql语句选择镶嵌线中特定的feature
//        option.add("-cwhere");
//        option.add(listSql.get(j));
//        option.add("-crop_to_cutline");
        //设置羽化宽度
        option.add("-cblend");
        option.add("0");

        //设置影像无效值区域像素值
        option.add("-srcnodata");
        option.add("0 0 0");
        Dataset[] tempInputDataset  = new Dataset[1];
        tempInputDataset[0] = mDataset;
        int nResult = gdal.Warp(destImg.mDataset, tempInputDataset, new WarpOptions( option));
        return true ;
    }

    public int ResampleImg(ImageXX destImg, int height, int width)
    {
        if (null == mDataset) return -1;
        Driver pDriver = gdal.GetDriverByName("MEM");
        if (null == pDriver)
        {
            return -2;
        }
        double[] dGeoTransform = mDataset.GetGeoTransform();
        double dOriResX = dGeoTransform[1];
        double dOriResY = dGeoTransform[5];
        double dNewResX = dOriResX * getM_nWidth()/width;
        double dNewResY = dOriResY * getM_nHeight()/height;
        double[] dNewGeoTransform = new double[6];
        dNewGeoTransform[0] = dGeoTransform[0];
        dNewGeoTransform[1] = dNewResX;
        dNewGeoTransform[2] = dGeoTransform[2];
        dNewGeoTransform[3] = dGeoTransform[3];
        dNewGeoTransform[4] = dGeoTransform[4];
        dNewGeoTransform[5] = dNewResY;
        destImg.mDataset = pDriver.Create("", width, height, getM_nChannels(), getM_nBitSize());
        if (null == destImg.mDataset) return  -3;
        destImg.mDataset.SetGeoTransform(dNewGeoTransform);
        destImg.mDataset.SetProjection(mDataset.GetProjection());
        int result = gdal.ReprojectImage(mDataset, destImg.mDataset, mDataset.GetProjection(), mDataset.GetProjection(), gdalconstConstants.GRA_NearestNeighbour);
        if (gdalconstConstants.CE_None == result)
        {
            destImg.Init();
        }
        return result;
    }



    //记录影像像素极值
    private int m_nMinValue = 0;
    private int m_nMaxValue = 255;

    public Boolean CreateImage(String sFilePath, int height, int width, int channal,
                               int bitsize)
    {
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO");
        String sExt = CommonTools.GetExt(sFilePath).toUpperCase();
        String pszFormat = CommonTools.GetImgDriverFormat(sExt);
        Driver pDriver = gdal.GetDriverByName(pszFormat);
        if (null == pDriver){
            System.out.println("GetDriverByName Failed!");
            return  false;
        }
        m_nHeight = height;
        m_nWidth = width;
        m_nChannels = channal;
        m_nBitSize = bitsize;
        pBand = new int[m_nChannels];
        for (int i=0; i<m_nChannels; i++)
        {
            pBand[i] = i+ 1;
        }

        mDataset = pDriver.Create(sFilePath, m_nWidth, m_nHeight, channal, bitsize);
        if (null == mDataset){
            System.out.println("Create Dataset Failed!");
            return  false;
        }

        return true;
    }

    /**
     * 创建影像
     * @param sFilePath：影像路径
     * @param env：影像外接矩形范围,数组[x0, y0, x1, y1]
     * @param xRes：影像x方向分辨率（正数）
     * @param yRes：影像y方向分辨率（正数）
     * @param channal：影像波段数
     * @param bitsize：影像像素位数（1-8位， 2-16位）
     * @return
     */
    public Boolean CreateImage(String sFilePath, double[] env, double xRes, double yRes, int channal,
                               int bitsize)
    {
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO");
        String sExt = "";
        if (!sFilePath.isEmpty())
        {
            sExt = CommonTools.GetExt(sFilePath).toUpperCase();
        }
        String pszFormat = CommonTools.GetImgDriverFormat(sExt);
        Driver pDriver = gdal.GetDriverByName(pszFormat);
        if (null == pDriver){
            System.out.println("GetDriverByName Failed!");
            return  false;
        }
        double d1 = ((env[2] - env[0])/xRes);
        m_nWidth = (int)Math.ceil((env[2] - env[0])/xRes);
        m_nHeight = (int)Math.ceil((env[3] - env[1])/yRes);
        double d2 = ((env[3] - env[1])/yRes);
        m_dXRes = xRes;
        m_dYRes = yRes;
        mEnv = env;
        m_nChannels = channal;
        m_nBitSize = bitsize;
        pBand = new int[m_nChannels];
        for (int i=0; i<m_nChannels; i++)
        {
            pBand[i] = i+ 1;
        }

        mDataset = pDriver.Create(sFilePath, m_nWidth, m_nHeight, channal, bitsize);
        if (null == mDataset){
            System.out.println("Create Dataset Failed!");
            return  false;
        }
        double[] dGeoTransform = new double[6];
        dGeoTransform[0] = env[0];
        dGeoTransform[1] = xRes;
        dGeoTransform[2] = 0.0;
        dGeoTransform[3] = env[3];
        dGeoTransform[4] = 0.0;
        dGeoTransform[5] = -1.0 * yRes;
        mDataset.SetGeoTransform(dGeoTransform);
        return true;
    }

    public void SetUpLeft(double ptX, double ptY, double xRes, double yRes)
    {
        m_dXRes = xRes;
        m_dYRes = yRes;

        mGeoTransform = new double[6];
        mGeoTransform[0] = ptX;
        mGeoTransform[1] = xRes;
        mGeoTransform[2] = 0;
        mGeoTransform[3] = ptY;
        mGeoTransform[4] = 0;
        mGeoTransform[5] = -yRes;

        mEnv = new double[4];
        mEnv[0] = mGeoTransform[0];
        mEnv[2] = mEnv[0] + m_nWidth*m_dXRes;
        mEnv[3] = mGeoTransform[3];
        mEnv[1] = mEnv[3] - m_nHeight*m_dYRes;
        mDataset.SetGeoTransform(mGeoTransform);
    }

    /**
     * 影像初始化
     * @param sFilePath
     * @return
     */
    public boolean Init(String sFilePath){
        mDataset = gdal.Open(sFilePath, gdalconstConstants.GA_ReadOnly);
        if (null == mDataset){
            System.out.println("Input Image Parse Failed!");
            return false;
        }
        Driver hDriver = mDataset.GetDriver();
        m_nWidth = mDataset.getRasterXSize();
        m_nHeight = mDataset.getRasterYSize();

        m_nChannels = mDataset.getRasterCount();
        Band band = mDataset.GetRasterBand(1);
        m_nBitSize =  band.getDataType();
        pBand = new int[m_nChannels];
        for (int i=0; i<m_nChannels; i++)
        {
            pBand[i] = i+ 1;
        }
        mGeoTransform = mDataset.GetGeoTransform();
        m_dXRes = mGeoTransform[1];
        m_dYRes = Math.abs( mGeoTransform[5]);
        mEnv = new double[4];
        mEnv[0] = mGeoTransform[0];
        mEnv[2] = mEnv[0] + m_nWidth*m_dXRes;
        mEnv[3] = mGeoTransform[3];
        mEnv[1] = mEnv[3] - m_nHeight*m_dYRes;
        return true;
    }

    public double[] GetTransform()
    {
        mGeoTransform = mDataset.GetGeoTransform();
        return mGeoTransform;
    }
    public void SetTransform(double[] dGeotransfom)
    {
        mGeoTransform = dGeotransfom;
        mDataset.SetGeoTransform(mGeoTransform);
    }

    public boolean Init(){
        if (null == mDataset){
            System.out.println("Input Image Parse Failed!");
            return false;
        }
        m_nWidth = mDataset.getRasterXSize();
        m_nHeight = mDataset.getRasterYSize();

        m_nChannels = mDataset.getRasterCount();
        Band band = mDataset.GetRasterBand(1);
        m_nBitSize =  band.getDataType();
        pBand = new int[m_nChannels];
        for (int i=0; i<m_nChannels; i++)
        {
            pBand[i] = i+ 1;
        }
        mGeoTransform = mDataset.GetGeoTransform();
        m_dXRes = mGeoTransform[1];
        m_dYRes = Math.abs( mGeoTransform[5]);
        mEnv = new double[4];
        mEnv[0] = mGeoTransform[0];
        mEnv[2] = mEnv[0] + m_nWidth*m_dXRes;
        mEnv[3] = mGeoTransform[3];
        mEnv[1] = mEnv[3] - m_nHeight*m_dYRes;
        return true;
    }





    /**
     * 将原始buf中的数据按照固定的波段序号写入到目标buf中
     * @param pSrcBuf：          原始像素块
     * @param pDstBuf：          目标像素块
     * @param index：            写入的波段序号
     * @param height：           待写入的数据行数
     * @param width：           待写入的数据列数
     * @return
     */
    public boolean SetValueAtIndex(int[] pSrcBuf, short[] pDstBuf, int index, int height, int width)
    {
        if (height*width * m_nChannels > pDstBuf.length)
        {
            System.out.println("function:SetValueAtIndex() Encounter Out of Memory Trouble!");
            return false;
        }
        for (int i=0; i<height; i++)
        {
            for (int j=0; j<width; j++)
            {
                pDstBuf[(height - i - 1)*width*m_nChannels + m_nChannels*j + index] =
                        (short) pSrcBuf[i*width + j];
            }
        }

        return true;
    }

    /**
     * 从原始buf中的数据按照固定的波段序号取出来写入到目标buf中
     * @param pSrcBuf：          原始像素块
     * @param pDstBuf：          目标像素块
     * @param index：            写入的波段序号
     * @param height：           待写入的数据行数
     * @param width：           待写入的数据列数
     * @return
     */
    public boolean GetValueAtIndex(short[] pSrcBuf, int[] pDstBuf, int index, int height, int width)
    {
        if (height*width * m_nChannels > pSrcBuf.length)
        {
            System.out.println("function:SetValueAtIndex() Encounter Out of Memory Trouble!");
            return false;
        }

        for (int i=0; i<height; i++)
        {
            for (int j=0; j<width; j++)
            {
                pDstBuf[i*width + j ] =
                        Math.max(pSrcBuf[(height - i - 1)*width*m_nChannels + m_nChannels*j + index], 0);

            }
        }

        return true;
    }
    /**
     * 从原始buf中的数据按照固定的波段序号取出来写入到目标buf中
     * @param pSrcBuf：          原始像素块
     * @param pDstBuf：          目标像素块
     * @param index：            写入的波段序号
     * @param height：           待写入的数据行数
     * @param width：           待写入的数据列数
     * @param nSrcStartRow：      原始像素块起始位置
     * @return
     */
    public boolean GetValueAtIndex(short[] pSrcBuf, int[] pDstBuf, int index, int height, int width, int nSrcStartRow)
    {
        if ((height + nSrcStartRow)*width * m_nChannels  > pSrcBuf.length)
        {
            System.out.println("function:SetValueAtIndex() Encounter Out of Memory Trouble!");
            return false;
        }
        if (height*width > pDstBuf.length)
        {
            System.out.println("function:SetValueAtIndex() Encounter Out of Memory Trouble!");
            return false;
        }
        for (int i=0; i<height; i++)
        {
            for (int j=0; j<width; j++)
            {
                pDstBuf[i*width + j ] =
                        Math.max(pSrcBuf[(height - i - 1 + nSrcStartRow)*width*m_nChannels + m_nChannels*j + index], 0);
            }
        }

        return true;
    }

    /**
     * 获取像素块
     * @param xOff：x方向起始位置
     * @param yOff：y方向起始位置
     * @param width：像素块宽度
     * @param height：像素高度
     * @param pBuf：存储像素块地址
     * @param nStartRow：存储像素块起始行号
     * @return
     */
    public boolean ReadBlock(int xOff, int yOff, int width, int height, short[] pBuf, int nStartRow){
        int nStartY = m_nHeight - height - yOff;
        if (xOff + width > m_nWidth || nStartY + height > m_nHeight) return false;

        if (height*width > mTempBuf.length)
        {
            System.out.println("ImageXX-ReadBlock()-mTempBuf: Out of Memory!");
            return false;
        }
        if ((nStartRow + height)*width*m_nChannels > pBuf.length )
        {
            System.out.println("ImageXX-ReadBlock()-mTempBuf: Out of Memory!");
            return false;
        }
        short[] pXXBuf = new short[height*width*m_nChannels];

        for (int i=0; i< m_nChannels; i++)
        {
            CommonTools.ClearBuf(mTempBuf, 0);
            Band band = mDataset.GetRasterBand(i + 1);
            band.ReadRaster(xOff, nStartY, width, height, mTempBuf);
            SetValueAtIndex(mTempBuf, pXXBuf, i, height, width);
        }

        for (int i=0; i<height; i++)
        {
            for (int j=0; j<width; j++)
            {
                for (int m=0; m<m_nChannels; m++)
                {
                    pBuf[(i + nStartRow)*width*m_nChannels + m_nChannels*j + m] = pXXBuf[i*width*m_nChannels + m_nChannels*j + m];
                }

            }
        }
        return  true;
    }


    /**
     * 获取像素块
     * @param xOff：x方向起始位置
     * @param yOff：y方向起始位置
     * @param width：像素块宽度
     * @param height：像素高度
     * @param pBuf：存储像素块地址
     * @return
     */
    public boolean ReadBlock(int xOff, int yOff, int width, int height, short[] pBuf){
        int nStartY = m_nHeight - height - yOff;
        int nEndY = m_nHeight - yOff;
        if (xOff + width > m_nWidth || nStartY + height > m_nHeight) return false;
        if (height*width > mTempBuf.length)
        {
            System.out.println("ImageXX-ReadBlock()-mTempBuf: Out of Memory!");
            return false;
        }
//        CommonTools.ClearBuf(pBuf, (short) 0);
        for (int i=0; i< m_nChannels; i++)
        {
            CommonTools.ClearBuf(mTempBuf, 0);
            Band band = mDataset.GetRasterBand(i + 1);
            band.ReadRaster(xOff, nStartY, width, height, mTempBuf);
            SetValueAtIndex(mTempBuf, pBuf, i, height, width);
        }
//        mDataset.ReadRaster(xOff, yOff , width, height, width, height,
//                gdalconstConstants.GDT_Byte, pBuf,pBand,
//                m_nBitSize*m_nChannels, m_nBitSize*m_nChannels*width);
        return  true;
    }


    public boolean TestReadBlock()
    {
        int[] pBuf = new int[m_nHeight * m_nWidth];
        for (int i=0; i<m_nChannels; i++)
        {
            for (int j = 0 ; j < pBuf.length; j++)
            {
                pBuf[j] = 0;
            }
            Band band = mDataset.GetRasterBand(i + 1);
            band.ReadRaster(0, 0, m_nWidth, m_nHeight, pBuf);
            int n1 = pBuf.length;
        }
        return true;
    }

    /**
     * 写入像素块
     * @param xOff：x方向起始位置
     * @param yOff：y方向起始位置
     * @param width：像素块宽度
     * @param height：像素高度
     * @param pBuf：存储像素块地址
     * @return
     */
    public boolean WriteBlock(int xOff, int yOff, int width, int height, short[] pBuf)
    {
        int nStartY = m_nHeight - height - yOff;
        if (xOff + width > m_nWidth || nStartY + height > m_nHeight) return false;
        if (height*width > mTempBuf.length)
        {
            System.out.println("ImageXX-WriteBlock()-mTempBuf: Out of Memory!");
            return false;
        }
        for (int i=0; i< m_nChannels; i++)
        {
            GetValueAtIndex(pBuf, mTempBuf, i, height, width);
            Band band = mDataset.GetRasterBand(i + 1);
            band.WriteRaster(xOff, nStartY, width, height, mTempBuf);
        }

        return true;
    }

    /**
     * 写入像素块
     * @param xOff：x方向起始位置
     * @param yOff：y方向起始位置
     * @param width：像素块宽度
     * @param height：像素高度
     * @param pBuf：存储像素块地址
     * @param nSrcStartRow：存储像素块起始位置
    //* @param pTempBuf：中间像素块地址
     * @return
     */
    public boolean WriteBlock(int xOff, int yOff, int width, int height, short[] pBuf, int nSrcStartRow)
    {
        int nStartY = m_nHeight - height - yOff;
        if (xOff + width > m_nWidth || nStartY + height > m_nHeight) return false;

        if (height*width > mTempBuf.length)
        {
            System.out.println("ImageXX-WriteBlock()-mTempBuf: Out of Memory!");
            return false;
        }
        for (int i=0; i< m_nChannels; i++)
        {
            GetValueAtIndex(pBuf, mTempBuf, i, height, width, nSrcStartRow);
            Band band = mDataset.GetRasterBand(i + 1);
            band.WriteRaster(xOff, nStartY, width, height, mTempBuf);
        }

        return true;
    }

    public boolean WriteBandBlock(int xOff, int yOff, int width, int height, int nBandIndex, short[] pBuf, int[] pTempBuf)
    {
        int nStartY = m_nHeight - height - yOff;
        if (xOff + width > m_nWidth || nStartY + height > m_nHeight) return false;
        if (nBandIndex <= 0 || nBandIndex > m_nChannels)return false;
        for (int i=0; i<height*width; i++)
        {
            pTempBuf[i] = pBuf[i];
        }
        Band band = mDataset.GetRasterBand(nBandIndex);
        band.WriteRaster(xOff, nStartY, width, height - 1, pTempBuf);
        return true;
    }



    public void SetWKT(String sWkt)
    {
        mDataset.SetProjection(sWkt);
    }



    /**
     * 获取影像外接矩形
     * @return
     */
    public double[] GetEnv(){
        return mEnv;
    }

    /**
     * 获取空间参考
     * @return
     */
    public String GetWKT()
    {
        return mDataset.GetProjection();
    }

    /**
     * 统计影像像素极值
     * @return
     */
    public boolean ComputeMinMax()
    {
        double[] arrMinMax = {0, 0};
        mDataset.GetRasterBand(1).ComputeRasterMinMax(arrMinMax, 1);
        m_nMinValue = (int)arrMinMax[0];
        m_nMaxValue = (int)arrMinMax[1];
        return true;
    }

    public void ReleaseObj()
    {
        if (null != mDataset)
            mDataset.delete();
    }
}