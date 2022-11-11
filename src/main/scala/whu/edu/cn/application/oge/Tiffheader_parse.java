package whu.edu.cn.application.oge;

import io.minio.MinioClient;
import io.minio.errors.*;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.GeographicCRS;
import org.opengis.referencing.crs.ProjectedCRS;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import geotrellis.raster.Tile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.gdal.gdalconst.gdalconstConstants.GDT_Byte;

public class Tiffheader_parse {
    public static class RawTile implements Serializable {
        String path;
        String time;
        String measurement;
        String product;
        double[] p_bottom_left = new double[2];/* extent of the tile*/
        double[] p_upper_right = new double[2];
        int[] row_col = new int[2];/* ranks of the tile*/
        long[] offset = new long[2];/* offsets of the tile*/
        byte[] tilebuf;/* byte stream of the tile*/
        int bitpersample;/* bit of the tile*/
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

        public double getP_bottom_leftX(){
            return this.p_bottom_left[0];
        }

        public double getP_bottom_leftY(){
            return this.p_bottom_left[1];
        }

        public double[] getP_upper_right() {
            return this.p_upper_right;
        }

        public double getP_upper_rightX(){
            return this.p_upper_right[0];
        }

        public double getP_upper_rightY(){
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

        public byte[] getTilebuf() {
            return this.tilebuf;
        }

        public int getBitpersample() {
            return this.bitpersample;
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

        public Tile getTile() { return this.tile;}

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

        public void setTilebuf(byte[] tilebuf) {
            this.tilebuf = tilebuf;
        }

        public void setBitpersample(int bitpersample) {
            this.bitpersample = bitpersample;
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

        public void setTile(Tile tile){ this.tile=tile;}
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
    private ArrayList<Double> GeoTrans = new ArrayList<>();
    private int xPosition;
    private int yPosition;

    public static ArrayList<RawTile> tileQuery(String in_path, String time, String crs, String measurement, String dType, String productName, double[] query_extent) {
        try {
            MinioClient minioClient = new MinioClient("http://125.220.153.26:9006", "rssample", "ypfamily608");
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
            Tiffheader_parse imageInfo=new Tiffheader_parse();
            imageInfo.parse(headerByte);
            return imageInfo.getTiles(query_extent, crs, in_path, time, measurement, dType, productName);

        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            System.out.println("Error occurred: " + e);
        }

        return null;
    }

    public static RawTile getTileBuf(RawTile tile) {
        try {
            MinioClient minioClient = new MinioClient("http://125.220.153.26:9006", "rssample", "ypfamily608");
            InputStream inputStream = minioClient.getObject("oge", tile.path, tile.offset[0], tile.offset[1] - tile.offset[0]);
            int length = Math.toIntExact((tile.offset[1] - tile.offset[0]));

            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[length];
            int len = -1;
            while ((len = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, len);
            }
            byte[] tilebuffer = outStream.toByteArray();
            tile.tilebuf = tilebuffer;
            inputStream.close();
            outStream.close();
            return tile;
        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            System.out.println("Error occurred: " + e);
        }
        return null;
    }

    private ArrayList<RawTile> getTiles(double[] query_extent, String crs, String in_path, String time, String measurement, String dType, String productName) {
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
                } else if (crsTarget instanceof GeographicCRS) {
                    flag = false;
                }
                MathTransform transform = CRS.findMathTransform(crsSource, crsTarget);
                JTS.transform(pointLower, pointLowerReprojected, transform);
                JTS.transform(pointUpper, pointUpperReprojected, transform);
            }
        } catch (FactoryException | TransformException e) {
            e.printStackTrace();
        }
        double pmin[] = new double[2];
        double pmax[] = new double[2];
        pmin[0] = pointLowerReprojected.getX();
        pmin[1] = pointLowerReprojected.getY();
        pmax[0] = pointUpperReprojected.getX();
        pmax[1] = pointUpperReprojected.getY();
        // 图像范围
        // 东西方向空间分辨率  --->像素宽度
        double w_src = this.Cell.get(0);
        // 南北方向空间分辨率 ---> 像素高度
        double h_src = this.Cell.get(1);
        // 左上角x坐标,y坐标 ---> 影像 左上角 投影坐标
        double xmin;
        double ymax;
        if (flag) {
            xmin = this.GeoTrans.get(3);
            ymax = this.GeoTrans.get(4);
        } else {
            xmin = this.GeoTrans.get(4);
            ymax = this.GeoTrans.get(3);
        }

        ArrayList<RawTile> tile_srch = new ArrayList<>();

        int l = 0;
        if ("MOD13Q1_061".equals(productName)) {
            l = 4;
            flagReader = true;
        }
        if ("LJ01_L2".equals(productName)) {
            flagReader = true;
        }
        if ("LC08_L1TP_C01_T1".equals(productName)) {
            l = 2;
        }
        if ("LE07_L1TP_C01_T1".equals(productName)) {
            l = 0;
        }


        //计算目标影像的左上和右下图上坐标
        int p_left;
        int p_right;
        int p_lower;
        int p_upper;
        if (flag) {
            p_left = (int) ((pmin[0] - xmin) / (256 * w_src * (int) Math.pow(2, l)));
            p_right = (int) ((pmax[0] - xmin) / (256 * w_src * (int) Math.pow(2, l)));
            p_lower = (int) ((ymax - pmax[1]) / (256 * h_src * (int) Math.pow(2, l)));
            p_upper = (int) ((ymax - pmin[1]) / (256 * h_src * (int) Math.pow(2, l)));
        } else {
            double xmax = xmin;
            double ymin = ymax;
            p_lower = (int) ((pmin[1] - ymin) / (256 * w_src * (int) Math.pow(2, l)));
            p_upper = (int) ((pmax[1] - ymin) / (256 * w_src * (int) Math.pow(2, l)));
            p_left = (int) ((xmax - pmax[0]) / (256 * h_src * (int) Math.pow(2, l)));
            p_right = (int) ((xmax - pmin[0]) / (256 * h_src * (int) Math.pow(2, l)));
        }
        if (!flagReader) {
            for (int i = (p_lower < 0 ? 0 : p_lower); i <= (p_upper >= TileOffsets.get(l).size() ? TileOffsets.get(l).size() - 1 : p_upper); i++) {
                for (int j = (p_left < 0 ? 0 : p_left); j <= (p_right >= TileOffsets.get(l).get(i).size() ? TileOffsets.get(l).get(i).size() - 1 : p_right); j++) {
                    RawTile t = new RawTile();
                    t.offset[0] = TileOffsets.get(l).get(i).get(j);
                    t.offset[1] = TileByteCounts.get(l).get(i).get(j) + t.offset[0];
                    t.p_bottom_left[0] = j * (256 * w_src * (int) Math.pow(2, l)) + xmin;
                    t.p_bottom_left[1] = (i + 1) * (256 * -h_src * (int) Math.pow(2, l)) + ymax;
                    t.p_upper_right[0] = (j + 1) * (256 * w_src * (int) Math.pow(2, l)) + xmin;
                    t.p_upper_right[1] = i * (256 * -h_src * (int) Math.pow(2, l)) + ymax;
                    t.rotation = GeoTrans.get(5);
                    t.resolution = w_src * (int) Math.pow(2, l);
                    t.row_col[0] = i;
                    t.row_col[1] = j;
                    t.bitpersample = BitPerSample;
                    t.level = l;
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
            for (int i = (p_left < 0 ? 0 : p_left); i <= (p_right >= TileOffsets.get(l).size() ? TileOffsets.get(l).size() - 1 : p_right); i++) {
                for (int j = (p_lower < 0 ? 0 : p_lower); j <= (p_upper >= TileOffsets.get(l).get(i).size() ? TileOffsets.get(l).get(i).size() - 1 : p_upper); j++) {
                    RawTile t = new RawTile();
                    double xmax = xmin;
                    double ymin = ymax;
                    t.offset[0] = TileOffsets.get(l).get(i).get(j);
                    t.offset[1] = TileByteCounts.get(l).get(i).get(j) + t.offset[0];
                    //对珞珈一号而言，是左上和右下
                    t.p_bottom_left[0] = (i + 1) * (256 * -w_src * (int) Math.pow(2, l)) + xmax;
                    t.p_bottom_left[1] = j * (256 * h_src * (int)Math.pow(2, l)) + ymin;
                    t.p_upper_right[0] = i * (256 * -w_src * (int) Math.pow(2, l)) + xmax;
                    t.p_upper_right[1] = (j + 1) * (256 * h_src * (int) Math.pow(2, l)) + ymin;
                    t.rotation = GeoTrans.get(5);
                    t.resolution = w_src * (int) Math.pow(2, l);
                    t.row_col[0] = i;
                    t.row_col[1] = j;
                    t.bitpersample = BitPerSample;
                    t.level = l;
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

    private double Getdouble(byte[] pd, int startPos, int Length) {
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
            double v = this.Getdouble(this.header, (startPos + i * typeSize), typeSize);
            this.GeoTrans.add(v);
        }
    }


    private void GetDoubleCell(int startPos, int typeSize, int count) {
        for (int i = 0; i < count; i++) {
            double v = this.Getdouble(this.header, (startPos + i * typeSize), typeSize);
            this.Cell.add(v);
        }
    }


    private String GetString(byte[] pd, int startPos, int Length) {
        byte[] dd = new byte[Length];
        System.arraycopy(pd, startPos, dd, 0, Length);
        String s = new String(dd);
        return s;
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
        ArrayList<ArrayList<Integer>> Stripbytes = new ArrayList<>();
        for (int i = 0; i < (this.ImageLength / 256) + 1; i++) {
            ArrayList<Integer> Tilebytes = new ArrayList<>();
            for (int j = 0; j < (this.ImageWidth / 256) + 1; j++) {
                int v = this.GetIntII(this.header, (startPos + (int) (i * ((this.ImageWidth / 256) + 1) + j) * typeSize), typeSize);
                Tilebytes.add(v);
            }
            Stripbytes.add(Tilebytes);
        }
        this.TileByteCounts.add(Stripbytes);
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
        int pNext = GetIntII(this.header, n, 4);
        return pNext;
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

    private void GetDEValue(int TagIndex, int TypeIndex, int Count, int pdata) {
        int typesize = TypeArray[TypeIndex];
        switch (TagIndex) {
            case 256://ImageWidth
                this.ImageWidth = GetIntII(this.header, pdata, typesize);
                this.ImageSize.add(this.ImageWidth);
                break;
            case 257://ImageLength
                this.ImageLength = GetIntII(this.header, pdata, typesize);
                this.ImageSize.add(this.ImageLength);
                break;
            case 258://ImageWidth
                this.BitPerSample = GetIntII(this.header, pdata, typesize);
                break;
            case 286://XPosition
                this.xPosition = GetIntII(this.header, pdata, typesize);
                break;
            case 287://YPosition
                this.yPosition = GetIntII(this.header, pdata, typesize);
                break;
            case 324: //TileOffsets
                this.GetOffsetArray(pdata, typesize, Count);
                break;
            case 325: //TileByteCounts
                this.GetTileBytesArray(pdata, typesize, Count);
                break;
            case 33550://  CellWidth
                this.GetDoubleCell(pdata, typesize, Count);
                break;
            case 33922: //GeoTransform
                this.GetDoubleTrans(pdata, typesize, Count);
                break;
            case 34737: //Spatial reference
                this.Srid = GetString(this.header, pdata, typesize * Count);
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

    private ArrayList<Double> getGeotrans() {
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

    public static String getpng(ArrayList<RawTile> tile_srch/*String in_path*/, ArrayList<Double> arrayList, String path, String path_img) {
        System.out.println("tile_srch.size() = " + tile_srch.size());

        int cols = (tile_srch.get(tile_srch.size() - 1).row_col[1] - tile_srch.get(0).row_col[1] + 1) * 256;
        int rows = (tile_srch.get(tile_srch.size() - 1).row_col[0] - tile_srch.get(0).row_col[0] + 1) * 256;

        String dstPath = path;
        gdal.AllRegister();
        Driver dr = gdal.GetDriverByName("PNG");
        Driver dr1 = gdal.GetDriverByName("MEM");
        Dataset dm = dr1.Create(dstPath, cols, rows, 1, GDT_Byte);
        //Dataset ds = dr.Create(dstPath,cols, rows, 1, GDT_Byte);

        for (int i = 0; i < tile_srch.size(); i++) {
            for (int j = tile_srch.get(0).row_col[0]; j <= tile_srch.get(tile_srch.size() - 1).row_col[0]; j++) {
                if (tile_srch.get(i).row_col[0] == j) {
                    int yoff = (tile_srch.get(i).row_col[1] - tile_srch.get(0).row_col[1]) * 256;
                    int xoff = (tile_srch.get(i).row_col[0] - tile_srch.get(0).row_col[0]) * 256;
                    dm.GetRasterBand(1).WriteRaster(yoff, xoff, 256, 256, 256, 256, GDT_Byte, tile_srch.get(i).tilebuf);
                    //ds.GetRasterBand(1).WriteRaster(yoff, xoff, 256, 256, 256, 256, GDT_Byte, tile_srch.get(i).tilebuf);
                }
            }
        }
        Dataset ds = dr.CreateCopy(dstPath, dm);
        return "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + arrayList.get(0) + "," + arrayList.get(1) + "," + arrayList.get(2) + "," + arrayList.get(3) + "]}]}";
    }
}
