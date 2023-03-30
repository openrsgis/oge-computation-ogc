package whu.edu.cn.application.oge;

import io.minio.MinioClient;
import io.minio.errors.*;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.ProjectedCRS;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import geotrellis.raster.Tile;
import redis.clients.jedis.Jedis;
import whu.edu.cn.util.JedisConnectionFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.gdal.gdalconst.gdalconstConstants.GDT_Byte;
import static whu.edu.cn.util.SystemConstants.*;



public class Tiffheader_parse {
    public static int nearestZoom = 0;

    public static class RawTile implements Serializable {
        String path;
        String time;
        String measurement;
        String product;
        double[] p_bottom_left = new double[2];/* extent of the tile*/
        double[] p_upper_right = new double[2];
        int[] row_col = new int[2];/* ranks of the tile*/
        long[] offset = new long[2];/* offsets of the tile*/
        byte[] tileBuf;/* byte stream of the tile*/
        int bitPerSample;/* bit of the tile*/
        int level;/* level of the overview*/
        double rotation;
        double resolution;
        int crs;
        String dType;
        Tile tile;

        public String getPath() {
            return this.path;
        }

        public String getTime() {
            return this.time;
        }

        public String getMeasurement() {
            return this.measurement;
        }

        public String getProduct() {
            return this.product;
        }

        public double[] getP_bottom_left() {
            return this.p_bottom_left;
        }

        public double getP_bottom_leftX() {
            return this.p_bottom_left[0];
        }

        public double getP_bottom_leftY() {
            return this.p_bottom_left[1];
        }

        public double[] getP_upper_right() {
            return this.p_upper_right;
        }

        public double getP_upper_rightX() {
            return this.p_upper_right[0];
        }

        public double getP_upper_rightY() {
            return this.p_upper_right[1];
        }

        public int[] getRow_col() {
            return this.row_col;
        }

        public int getRow() {
            return this.row_col[0];
        }

        public int getCol() {
            return this.row_col[1];
        }

        public long[] getOffset() {
            return this.offset;
        }

        public byte[] getTileBuf() {
            return this.tileBuf;
        }

        public int getBitPerSample() {
            return this.bitPerSample;
        }

        public int getLevel() {
            return this.level;
        }

        public double getRotation() {
            return this.rotation;
        }

        public double getResolution() {
            return this.resolution;
        }

        public int getCrs() {
            return this.crs;
        }

        public String getDType() {
            return this.dType;
        }

        public Tile getTile() {
            return this.tile;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public void setTime(String time) {
            this.time = time;
        }

        public void setMeasurement(String measurement) {
            this.measurement = measurement;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public void setP_bottom_left(double[] p_bottom_left) {
            this.p_bottom_left = p_bottom_left;
        }

        public void setP_upper_right(double[] p_upper_right) {
            this.p_upper_right = p_upper_right;
        }

        public void setRow_col(int[] row_col) {
            this.row_col = row_col;
        }

        public void setRow(int row) {
            this.row_col[0] = row;
        }

        public void setCol(int col) {
            this.row_col[1] = col;
        }

        public void setOffset(long[] offset) {
            this.offset = offset;
        }

        public void setTileBuf(byte[] tileBuf) {
            this.tileBuf = tileBuf;
        }

        public void setBitPerSample(int bitPerSample) {
            this.bitPerSample = bitPerSample;
        }

        public void setLevel(int level) {
            this.level = level;
        }

        public void setRotation(double rotation) {
            this.rotation = rotation;
        }

        public void setResolution(double resolution) {
            this.resolution = resolution;
        }

        public void setCrs(int crs) {
            this.crs = crs;
        }

        public void setDType(String dType) {
            this.dType = dType;
        }

        public void setTile(Tile tile) {
            this.tile = tile;
        }
    }

    private ArrayList<ArrayList<ArrayList<Integer>>> TileOffsets = new ArrayList<>();
    private ArrayList<ArrayList<ArrayList<Integer>>> TileByteCounts = new ArrayList<>();
    private byte[] header = new byte[16383];
    private String Srid;
    private int BitPerSample;
    private int ImageWidth;
    private int ImageLength;
    private ArrayList<Integer> ImageSize = new ArrayList<Integer>();
    private ArrayList<Double> Cell = new ArrayList<>();
    private ArrayList<Double> GeoTrans = new ArrayList<>();//TODO 经纬度
    private int xPosition;
    private int yPosition;

    public static ArrayList<RawTile> tileQuery(int level, String in_path,
                                               String time, String crs, String measurement,
                                               String dType, String resolution, String productName,
                                               double[] query_extent) {
        try {
            MinioClient minioClient = new MinioClient(MINIO_URL, MINIO_KEY, MINIO_PWD);
            // 获取指定offset和length的"myobject"的输入流。
            InputStream inputStream = minioClient.getObject("oge", in_path, 0L, 262143L);
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[262143];//16383
            int len = -1;
            while ((len = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, len);
            }

            byte[] headerByte = outStream.toByteArray();
            inputStream.close();
            outStream.close();

            // 出于性能的考虑，静态变量序列化会比较慢
            Tiffheader_parse imageInfo = new Tiffheader_parse();
            imageInfo.parse(headerByte);
            return imageInfo.getTiles(level, query_extent, crs, in_path, time, measurement, dType, resolution, productName);

        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            System.out.println("Error occurred: " + e);
        }

        return null;
    }

    public static RawTile getTileBuf(RawTile tile) {
        try {
            MinioClient minioClient = new MinioClient(MINIO_URL, MINIO_KEY, MINIO_PWD);
            InputStream inputStream = minioClient.getObject("oge", tile.path, tile.offset[0], tile.offset[1] - tile.offset[0]);
            int length = Math.toIntExact((tile.offset[1] - tile.offset[0]));

            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[length];
            int len = -1;
            while ((len = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, len);
            }
            byte[] tilebuffer = outStream.toByteArray();
            tile.tileBuf = tilebuffer;
            inputStream.close();
            outStream.close();
            return tile;
        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            System.out.println("Error occurred: " + e);
        }
        return null;
    }

    /**
     * @param l            json里的 level 字段，表征前端 Zoom
     * @param query_extent
     * @param crs
     * @param in_path
     * @param time
     * @param measurement
     * @param dType
     * @param resolution   数据库中的原始影像分辨率
     * @param productName
     * @return
     */
    private ArrayList<RawTile> getTiles(int l, double[] query_extent,
                                        String crs, String in_path, String time,
                                        String measurement, String dType,
                                        String resolution, String productName) {
        int level;
        double resolutionTMS = 0.0;
        double[] resolutionTMSArray = {
                156543.033928, /* 地图 zoom 为0时的分辨率，以下按zoom递增 */
                78271.516964,
                39135.758482,
                19567.879241,
                9783.939621,
                4891.969810,
                2445.984905,
                1222.992453,
                611.496226,
                305.748113,
                152.874057,
                76.437028,
                38.218514,
                19.109257,
                9.554629,
                4.777314,
                2.388657,
                1.194329,
                0.597164,
                0.298582,
                0.149291
        };
        double resolutionOrigin = Double.parseDouble(resolution);
        System.out.println("resolutionOrigin = " + resolutionOrigin);//460
        if (l == -1) {
            level = 0;
        }
        else {
            resolutionTMS = resolutionTMSArray[l];
            System.out.println(l);
            level = (int) Math.ceil(Math.log(resolutionTMS / resolutionOrigin) / Math.log(2)) + 1;
            int maxZoom = 0;
            for (int i = 0; i < resolutionTMSArray.length; i++) {
                if ((int) Math.ceil(Math.log(resolutionTMSArray[i] / resolutionOrigin) / Math.log(2)) + 1 == 0) {
                    maxZoom = i;
                    break;
                }
            }

            System.out.println("maxZoom = " + maxZoom);

            System.out.println("java.level = " + level);
            System.out.println("TileOffsets.size() = " + TileOffsets.size()); // 后端瓦片数

            // 正常情况下的换算关系
            Tiffheader_parse.nearestZoom = ImageTrigger.level();
            //TODO 这里我们认为数据库中金字塔的第0层对应了前端 zoom 的第10级
//                        0 10
//                        1 9
//                        2 8
//                        ...
//                        6 4

            if (level > TileOffsets.size() - 1) {
                level = TileOffsets.size() - 1;
                assert maxZoom > level;

                Tiffheader_parse.nearestZoom = maxZoom - level;

                // throw new RuntimeException("Level is too small!");
            }
            if (level < 0) {
                level = 0;
                Tiffheader_parse.nearestZoom = maxZoom;

                // throw new RuntimeException("Level is too big!");
            }
        }

        double lower_left_long = query_extent[0];
        double lower_left_lat = query_extent[1];
        double upper_right_long = query_extent[2];
        double upper_right_lat = query_extent[3];

        Coordinate pointLower = new Coordinate();
        Coordinate pointUpper = new Coordinate();
        pointLower.setX(lower_left_lat);
        pointLower.setY(lower_left_long);
        pointUpper.setX(upper_right_lat);
        pointUpper.setY(upper_right_long);
        Coordinate pointLowerReprojected = new Coordinate();
        Coordinate pointUpperReprojected = new Coordinate();
        boolean flag = false;
        boolean flagReader = false;
        try {
            if ("EPSG:4326".equals(crs)) {
                pointLowerReprojected = pointLower;
                pointUpperReprojected = pointUpper;
            } else {
                CoordinateReferenceSystem crsSource = CRS.decode("EPSG:4326");
                //System.out.println("是否为投影坐标"+(crsSource instanceof ProjectedCRS));
                //System.out.println("是否为经纬度坐标"+(crsSource instanceof GeographicCRS));
                CoordinateReferenceSystem crsTarget = CRS.decode(crs);
                if (crsTarget instanceof ProjectedCRS) {
                    flag = true;
                }

                MathTransform transform = CRS.findMathTransform(crsSource, crsTarget);
                JTS.transform(pointLower, pointLowerReprojected, transform);
                JTS.transform(pointUpper, pointUpperReprojected, transform);
            }
        } catch (FactoryException |
                 TransformException e) {
            e.printStackTrace();
        }

        double[] pMin = new double[2];
        double[] pMax = new double[2];
        pMin[0] = pointLowerReprojected.getX();
        pMin[1] = pointLowerReprojected.getY();
        pMax[0] = pointUpperReprojected.getX();
        pMax[1] = pointUpperReprojected.getY();
        // 图像范围
        // 东西方向空间分辨率  --->像素宽度
        double w_src = this.Cell.get(0);
        // 南北方向空间分辨率 ---> 像素高度 // TODO
        double h_src = this.Cell.get(1);
        // 左上角x坐标,y坐标 ---> 影像 左上角 投影坐标
        double xMin;
        double yMax;

        if (flag) {
            xMin = this.GeoTrans.get(3);
            yMax = this.GeoTrans.get(4);
        } else {
            xMin = this.GeoTrans.get(4);
            yMax = this.GeoTrans.get(3);
        }

        ArrayList<RawTile> tile_srch = new ArrayList<>();

        //int l = 0;
/*        if ("MOD13Q1_061".equals(productName)) {
            flagReader = true;
        }
        if ("LJ01_L2".equals(productName)) {
            flagReader = true;
        }
        if ("ASTER_GDEM_DEM30".equals(productName)) {
            flagReader = true;
        }
        if ("GPM_Precipitation_China_Month".equals(productName)) {
            flagReader = true;
        }*/
        switch (productName) {
            case "MOD13Q1_061":
            case "LJ01_L2":
            case "ASTER_GDEM_DEM30":
            case "GPM_Precipitation_China_Month":
                flagReader = true;
                break;
            default:
                break;

        }


        //if ("LC08_L1TP_C01_T1".equals(productName)) {
        //    l = 2;
        //}
        //if ("LE07_L1TP_C01_T1".equals(productName)) {
        //    l = 0;
        //}

        //计算目标影像的左上和右下图上坐标
        int p_left;
        int p_right;
        int p_lower;
        int p_upper;
        if (flag) {
            p_left = (int) ((pMin[0] - xMin) / (256 * w_src * (int) Math.pow(2, level)));
            p_right = (int) ((pMax[0] - xMin) / (256 * w_src * (int) Math.pow(2, level)));
            p_lower = (int) ((yMax - pMax[1]) / (256 * h_src * (int) Math.pow(2, level)));
            p_upper = (int) ((yMax - pMin[1]) / (256 * h_src * (int) Math.pow(2, level)));
        } else {
            p_lower = (int) ((pMin[1] - yMax) / (256 * w_src * (int) Math.pow(2, level)));
            p_upper = (int) ((pMax[1] - yMax) / (256 * w_src * (int) Math.pow(2, level)));
            p_left = (int) ((xMin - pMax[0]) / (256 * h_src * (int) Math.pow(2, level)));
            p_right = (int) ((xMin - pMin[0]) / (256 * h_src * (int) Math.pow(2, level)));
        }


        int[] pCoordinate = null;
        double[] srcSize = null;
        double[] pRange = null;
        if (!flagReader) {
            pCoordinate = new int[]{p_lower, p_upper, p_left, p_right};
            srcSize = new double[]{w_src, h_src};
            pRange = new double[]{xMin, yMax};
        } else {
            pCoordinate = new int[]{p_left, p_right, p_lower, p_upper};
            srcSize = new double[]{h_src, w_src};
            pRange = new double[]{yMax, xMin};
        }


        for (int i = (Math.max(pCoordinate[0], 0));
             i <= (pCoordinate[1] >= TileOffsets.get(level).size() ?
                     TileOffsets.get(level).size() - 1 : pCoordinate[1]);
             i++
        ) {
            for (int j = (Math.max(pCoordinate[2], 0));
                 j <= (pCoordinate[3] >= TileOffsets.get(level).get(i).size() ?
                         TileOffsets.get(level).get(i).size() - 1 : pCoordinate[3]);
                 j++) {
                RawTile t = new RawTile();
                t.offset[0] = TileOffsets.get(level).get(i).get(j);
                t.offset[1] = TileByteCounts.get(level).get(i).get(j) + t.offset[0];
                t.p_bottom_left[0] = j *
                        (256 * srcSize[0] * (int) Math.pow(2, level)) +
                        pRange[0];
                t.p_bottom_left[1] = (i + 1) *
                        (256 * -srcSize[1] * (int) Math.pow(2, level)) +
                        pRange[1];
                t.p_upper_right[0] = (j + 1) *
                        (256 * srcSize[0] * (int) Math.pow(2, level)) +
                        pRange[0];
                t.p_upper_right[1] = i *
                        (256 * -srcSize[1] * (int) Math.pow(2, level)) +
                        pRange[1];
                t.rotation = GeoTrans.get(5);
                t.resolution = w_src * (int) Math.pow(2, level);
                t.row_col[0] = i;
                t.row_col[1] = j;
                t.bitPerSample = BitPerSample;
                t.level = level;
                t.path = in_path;
                t.time = time;
                t.measurement = measurement;
                t.crs = Integer.parseInt(crs.replace("EPSG:", ""));
                t.dType = dType;
                t.product = productName;
                tile_srch.add(t);


            }
        }


/*
        if (!flagReader) {
            for (int i = (Math.max(p_lower, 0));
                 i <= (p_upper >= TileOffsets.get(level).size() ?
                         TileOffsets.get(level).size() - 1 : p_upper);
                 i++
            ) {
                for (int j = (Math.max(p_left, 0));
                     j <= (p_right >= TileOffsets.get(level).get(i).size() ?
                             TileOffsets.get(level).get(i).size() - 1 : p_right);
                     j++) {
                    RawTile t = new RawTile();
                    t.offset[0] = TileOffsets.get(level).get(i).get(j);
                    t.offset[1] = TileByteCounts.get(level).get(i).get(j) + t.offset[0];
                    t.p_bottom_left[0] = j * (256 * w_src * (int) Math.pow(2, level)) + xMin;
                    t.p_bottom_left[1] = (i + 1) * (256 * -h_src * (int) Math.pow(2, level)) + yMax;
                    t.p_upper_right[0] = (j + 1) * (256 * w_src * (int) Math.pow(2, level)) + xMin;
                    t.p_upper_right[1] = i * (256 * -h_src * (int) Math.pow(2, level)) + yMax;
                    t.rotation = GeoTrans.get(5);
                    t.resolution = w_src * (int) Math.pow(2, level);
                    t.row_col[0] = i;
                    t.row_col[1] = j;
                    t.bitpersample = BitPerSample;
                    t.level = level;
                    t.path = in_path;
                    t.time = time;
                    t.measurement = measurement;
                    t.crs = Integer.parseInt(crs.replace("EPSG:", ""));
                    t.dType = dType;
                    t.product = productName;
                    tile_srch.add(t);
                }
            }
        } else {
            for (int i = (Math.max(p_left, 0));
                 i <= (p_right >= TileOffsets.get(level).size() ?
                         TileOffsets.get(level).size() - 1 : p_right);
                 i++) {
                for (int j = (Math.max(p_lower, 0));
                     j <= (p_upper >= TileOffsets.get(level).get(i).size() ?
                             TileOffsets.get(level).get(i).size() - 1 : p_upper);
                     j++) {
                    RawTile t = new RawTile();
                    t.offset[0] = TileOffsets.get(level).get(i).get(j);
                    t.offset[1] = TileByteCounts.get(level).get(i).get(j) + t.offset[0];
                    //对珞珈一号而言，是左上和右下
                    //t.p_bottom_left[0] = (i + 1) * (256 * -w_src * (int) Math.pow(2, level)) + xmax;
                    //t.p_bottom_left[1] = j * (256 * h_src * (int)Math.pow(2, level)) + ymin;
                    //t.p_upper_right[0] = i * (256 * -w_src * (int) Math.pow(2, level)) + xmax;
                    //t.p_upper_right[1] = (j + 1) * (256 * h_src * (int) Math.pow(2, level)) + ymin;
                    //t.rotation = GeoTrans.get(5);
                    //t.resolution = w_src * (int) Math.pow(2, level);
                    //t.row_col[0] = i;
                    //t.row_col[1] = j;
                    t.p_bottom_left[0] = j * (256 * h_src * (int) Math.pow(2, level)) + yMax;
                    t.p_bottom_left[1] = (i + 1) * (256 * -w_src * (int) Math.pow(2, level)) + xMin;
                    t.p_upper_right[0] = (j + 1) * (256 * h_src * (int) Math.pow(2, level)) + yMax;
                    t.p_upper_right[1] = i * (256 * -w_src * (int) Math.pow(2, level)) + xMin;
                    t.rotation = GeoTrans.get(5);
                    t.resolution = w_src * (int) Math.pow(2, level);
                    t.row_col[0] = i;
                    t.row_col[1] = j;
                    t.bitpersample = BitPerSample;
                    t.level = level;
                    t.path = in_path;
                    t.time = time;
                    t.measurement = measurement;
                    t.crs = Integer.parseInt(crs.replace("EPSG:", ""));
                    t.dType = dType;
                    t.product = productName;
                    tile_srch.add(t);
                }
            }
        }
        */

        return tile_srch;
    }

    private static final int[] TypeArray = {//"???",
            0,
            1,//byte //8-bit unsigned integer
            1,//ascii//8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero)
            2, //short",2),//16-bit (2-byte) unsigned integer.
            4,//  long",4),//32-bit (4-byte) unsigned integer.
            8,//  rational",8),//Two LONGs: the first represents the numerator of a fraction; the second, the denominator.
            1,//  sbyte",1),//An 8-bit signed (twos-complement) integer
            1,//   undefined",1),//An 8-bit byte that may contain anything, depending on the definition of the field
            2,//   sshort",1),//A 16-bit (2-byte) signed (twos-complement) integer.
            4,//   slong",1),// A 32-bit (4-byte) signed (twos-complement) integer.
            8,//    srational",1),//Two SLONG’s: the first represents the numerator of a fraction, the second the denominator.
            4,//    float",4),//Single precision (4-byte) IEEE format
            8//    double",8)//Double precision (8-byte) IEEE format
    };


    private int GetIntII(byte[] pd, int startPos, int Length) {
        int value = 0;
        for (int i = 0; i < Length; i++) {
            value |= pd[startPos + i] << i * 8;
            if (value < 0) {
                value += 256 << i * 8;
            }
        }
        return value;
    }

    private double GetDouble(byte[] pd, int startPos, int Length) {
        long value = 0;
        for (int i = 0; i < Length; i++) {
            value |= ((long) (pd[startPos + i] & 0xff)) << (8 * i);
            if (value < 0) {
                value += (long) 256 << i * 8;
            }
        }
        return Double.longBitsToDouble(value);
    }

    private void GetDoubleTrans(int startPos, int typeSize, int count) {
        for (int i = 0; i < count; i++) {
            double v = this.GetDouble(this.header, (startPos + i * typeSize), typeSize);
            this.GeoTrans.add(v);
        }
    }


    private void GetDoubleCell(int startPos, int typeSize, int count) {
        for (int i = 0; i < count; i++) {
            double v = this.GetDouble(this.header, (startPos + i * typeSize), typeSize);
            this.Cell.add(v);
        }
    }


    private String GetString(byte[] pd, int startPos, int Length) {
        byte[] dd = new byte[Length];
        System.arraycopy(pd, startPos, dd, 0, Length);
        return new String(dd);
    }

    private void GetOffsetArray(int startPos, int typeSize, int count) {
        ArrayList<ArrayList<Integer>> StripOffsets = new ArrayList<>();
        for (int i = 0; i < (this.ImageLength / 256) + 1; i++) {
            ArrayList<Integer> Offsets = new ArrayList<>();
            for (int j = 0; j < (this.ImageWidth / 256) + 1; j++) {
                int v = this.GetIntII(this.header, (startPos + (int) (i * ((this.ImageWidth / 256) + 1) + j) * typeSize), typeSize);
                Offsets.add(v);
            }
            StripOffsets.add(Offsets);
        }
        this.TileOffsets.add(StripOffsets);
    }

    private void GetTileBytesArray(int startPos, int typeSize, int count) {
        ArrayList<ArrayList<Integer>> stripBytes = new ArrayList<>();
        for (int i = 0; i < (this.ImageLength / 256) + 1; i++) {
            ArrayList<Integer> tileBytes = new ArrayList<>();
            for (int j = 0; j < (this.ImageWidth / 256) + 1; j++) {
                int v = this.GetIntII(this.header, (startPos + (int) (i * ((this.ImageWidth / 256) + 1) + j) * typeSize), typeSize);
                tileBytes.add(v);
            }
            stripBytes.add(tileBytes);
        }
        this.TileByteCounts.add(stripBytes);
    }

    private int DecodeIFH() {
        return GetIntII(this.header, 4, 4);
    }

    private int DecodeIFD(int n) {
        int DECount = GetIntII(this.header, n, 2);
        n += 2;
        for (int i = 0; i < DECount; i++) {
            this.DecodeDE(n);
            n += 12;
        }
        return GetIntII(this.header, n, 4);
    }

    private void DecodeDE(int pos) {
        int TagIndex = GetIntII(this.header, pos, 2);
        int TypeIndex = GetIntII(this.header, pos + 2, 2);
        int Count = GetIntII(this.header, pos + 4, 4);

        //先找到数据的位置
        int pData = pos + 8;
        int totalSize = TypeArray[TypeIndex] * Count;
        if (totalSize > 4) {
            pData = GetIntII(this.header, pData, 4);
        }

        //再根据Tag把值读出并存起来
        this.GetDEValue(TagIndex, TypeIndex, Count, pData);
    }

    private void GetDEValue(int tagIndex, int typeIndex, int count, int pData) {
        int typeSize = TypeArray[typeIndex];
        switch (tagIndex) {
            case 256://ImageWidth
                this.ImageWidth = GetIntII(this.header, pData, typeSize);
                this.ImageSize.add(this.ImageWidth);
                break;
            case 257://ImageLength
                this.ImageLength = GetIntII(this.header, pData, typeSize);
                this.ImageSize.add(this.ImageLength);
                break;
            case 258://ImageWidth
                this.BitPerSample = GetIntII(this.header, pData, typeSize);
                break;
            case 286://XPosition
                this.xPosition = GetIntII(this.header, pData, typeSize);
                break;
            case 287://YPosition
                this.yPosition = GetIntII(this.header, pData, typeSize);
                break;
            case 324: //TileOffsets
                this.GetOffsetArray(pData, typeSize, count);
                break;
            case 325: //TileByteCounts
                this.GetTileBytesArray(pData, typeSize, count);
                break;
            case 33550://  CellWidth
                this.GetDoubleCell(pData, typeSize, count);
                break;
            case 33922: //GeoTransform
                this.GetDoubleTrans(pData, typeSize, count);
                break;
            case 34737: //Spatial reference
                this.Srid = GetString(this.header, pData, typeSize * count);
                break;
            default:
                break;
        }
    }

    private void parse(byte[] stream) {
        this.header = stream;
        int pIFD = this.DecodeIFH();
        while (pIFD != 0) {
            pIFD = this.DecodeIFD(pIFD);
        }
    }

    private String getSrid() {
        return this.Srid;
    }

    private ArrayList<Integer> getImageSize() {
        return this.ImageSize;
    }

    private ArrayList<Double> getGeoTrans() {
        return this.GeoTrans;
    }

    private ArrayList<Double> getCell() {
        return this.Cell;
    }

    private int getBit() {
        return this.BitPerSample;
    }

    public static ArrayList<Double> getBounds(List<ArrayList<Double>> tile_extents) {
        Coordinate pointLower = new Coordinate();
        Coordinate pointUpper = new Coordinate();
        pointLower.setX(tile_extents.get(0).get(0) - 15);
        pointLower.setY(tile_extents.get(tile_extents.size() - 1).get(1) - 15);
        pointUpper.setX(tile_extents.get(tile_extents.size() - 1).get(2) + 15);
        pointUpper.setY(tile_extents.get(0).get(3) + 15);
        Coordinate pointLowerReprojected = new Coordinate();
        Coordinate pointUpperReprojected = new Coordinate();
        CoordinateReferenceSystem crsSource = null;
        try {
            crsSource = CRS.decode("EPSG:32649");
        } catch (FactoryException e) {
            e.printStackTrace();
        }
        CoordinateReferenceSystem crsTarget = null;
        try {
            crsTarget = CRS.decode("EPSG:4326");
        } catch (FactoryException e) {
            e.printStackTrace();
        }
        MathTransform transform = null;
        try {
            transform = CRS.findMathTransform(crsSource, crsTarget);
        } catch (FactoryException e) {
            e.printStackTrace();
        }
        try {
            assert transform != null; //
            JTS.transform(pointLower, pointLowerReprojected, transform);
        } catch (TransformException e) {
            e.printStackTrace();
        }
        try {
            JTS.transform(pointUpper, pointUpperReprojected, transform);
        } catch (TransformException e) {
            e.printStackTrace();
        }

        ArrayList<Double> arrayList = new ArrayList();
        arrayList.add(pointLowerReprojected.x);
        arrayList.add(pointLowerReprojected.y);
        arrayList.add(pointUpperReprojected.x);
        arrayList.add(pointUpperReprojected.y);
        return arrayList;
    }

    public static String getpng(ArrayList<RawTile> tile_srch/*String in_path*/, ArrayList<Double> arrayList, String path/* dstPath */, String path_img) {
        System.out.println("tile_srch.size() = " + tile_srch.size());

        int cols = (tile_srch.get(tile_srch.size() - 1).row_col[1] - tile_srch.get(0).row_col[1] + 1) * 256;
        int rows = (tile_srch.get(tile_srch.size() - 1).row_col[0] - tile_srch.get(0).row_col[0] + 1) * 256;

        gdal.AllRegister();
        Driver dr = gdal.GetDriverByName("PNG");
        Driver dr1 = gdal.GetDriverByName("MEM");
        Dataset dm = dr1.Create(path, cols, rows, 1, GDT_Byte);
        //Dataset ds = dr.Create(dstPath,cols, rows, 1, GDT_Byte);

        for (int i = 0; i < tile_srch.size(); i++) {
            for (int j = tile_srch.get(0).row_col[0]; j <= tile_srch.get(tile_srch.size() - 1).row_col[0]; j++) {
                if (tile_srch.get(i).row_col[0] == j) {
                    int yOff = (tile_srch.get(i).row_col[1] - tile_srch.get(0).row_col[1]) * 256;
                    int xOff = (tile_srch.get(i).row_col[0] - tile_srch.get(0).row_col[0]) * 256;
                    dm.GetRasterBand(1).WriteRaster(yOff, xOff, 256, 256, 256, 256, GDT_Byte, tile_srch.get(i).tileBuf);
                    //ds.GetRasterBand(1).WriteRaster(yoff, xoff, 256, 256, 256, 256, GDT_Byte, tile_srch.get(i).tilebuf);
                }
            }
        }
        Dataset ds = dr.CreateCopy(path, dm);
        return "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + arrayList.get(0) + "," + arrayList.get(1) + "," + arrayList.get(2) + "," + arrayList.get(3) + "]}]}";
    }




}
