package whu.edu.cn.geocube.application.tritonClient;
//
//import io.minio.MinioClient;
//import io.minio.errors.*;
//import org.gdal.gdal.Dataset;
//import org.gdal.gdal.Driver;
//import org.gdal.gdal.gdal;
//import org.geotools.geometry.jts.JTS;
//import org.geotools.referencing.CRS;
//import org.locationtech.jts.geom.Coordinate;
//import org.opengis.referencing.FactoryException;
//import org.opengis.referencing.crs.CoordinateReferenceSystem;
//import org.opengis.referencing.operation.MathTransform;
//import org.opengis.referencing.operation.TransformException;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.Serializable;
//import java.security.InvalidKeyException;
//import java.security.NoSuchAlgorithmException;
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.gdal.gdalconst.gdalconstConstants.GDT_Byte;
//
public class Tiffheader_parse_DL {}
//    public static class RawTile implements Serializable {
//        String path;
//        String time;
//        String measurement;
//        double[] p_bottom_left = new double[2];/* extent of the tile*/
//        double[] p_upper_right = new double[2];
//        int[] row_col = new int[2];/* ranks of the tile*/
//        long[] offset = new long[2];/* offsets of the tile*/
//        byte[] tilebuf;/* byte stream of the tile*/
//        int bitpersample;/* bit of the tile*/
//        int samples;/* samples of the pixel*/
//        int level;/* level of the overview*/
//        double rotation;
//        double resolution;
//
//        public String getPath() {
//            return path;
//        }
//
//        public String getTime() {
//            return time;
//        }
//
//        public String getMeasurement() {
//            return measurement;
//        }
//
//        public double[] getP_bottom_left() {
//            return p_bottom_left;
//        }
//
//        public double getP_bottom_leftX() {
//            return p_bottom_left[0];
//        }
//
//        public double getP_bottom_leftY() {
//            return p_bottom_left[1];
//        }
//
//        public double[] getP_upper_right() {
//            return p_upper_right;
//        }
//
//        public double getP_upper_rightX() {
//            return p_upper_right[0];
//        }
//
//        public double getP_upper_rightY() {
//            return p_upper_right[1];
//        }
//
//        public int[] getRow_col() {
//            return row_col;
//        }
//
//        public int getRow() {
//            return row_col[0];
//        }
//
//        public int getCol() {
//            return row_col[1];
//        }
//
//        public long[] getOffset() {
//            return offset;
//        }
//
//        public byte[] getTilebuf() {
//            return tilebuf;
//        }
//
//        public int getSamples() {
//            return samples;
//        }
//
//        public int getBitpersample() {
//            return bitpersample;
//        }
//
//        public int getLevel() {
//            return level;
//        }
//
//        public double getRotation() {
//            return rotation;
//        }
//
//        public double getResolution() {
//            return resolution;
//        }
//
//        public void setPath(String path) {
//            this.path = path;
//        }
//
//        public void setTime(String time) {
//            this.time = time;
//        }
//
//        public void setMeasurement(String measurement) {
//            this.measurement = measurement;
//        }
//
//        public void setP_bottom_left(double[] p_bottom_left) {
//            this.p_bottom_left = p_bottom_left;
//        }
//
//        public void setP_upper_right(double[] p_upper_right) {
//            this.p_upper_right = p_upper_right;
//        }
//
//        public void setRow_col(int[] row_col) {
//            this.row_col = row_col;
//        }
//
//        public void setRow(int row) {
//            this.row_col[0] = row;
//        }
//
//        public void setCol(int col) {
//            this.row_col[1] = col;
//        }
//
//        public void setOffset(long[] offset) {
//            this.offset = offset;
//        }
//
//        public void setTilebuf(byte[] tilebuf) {
//            this.tilebuf = tilebuf;
//        }
//
//        public void setBitpersample(int bitpersample) {
//            this.bitpersample = bitpersample;
//        }
//
//        public void setLevel(int level) {
//            this.level = level;
//        }
//
//        public void setRotation(double rotation) {
//            this.rotation = rotation;
//        }
//
//        public void setResolution(double resolution) {
//            this.resolution = resolution;
//        }
//    }
//
//    public static ArrayList<RawTile> tileQuery(int level, String in_path, String time, String srcID, String measurement, double[] query_extent) {
//        try {
//            MinioClient minioClient = new MinioClient("http://125.220.153.26:9006", "rssample", "ypfamily608");
//            // 获取指定offset和length的"myobject"的输入流。
//            InputStream inputStream = minioClient.getObject("oge", in_path, 0L, 5116L);
//            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
//            byte[] buffer = new byte[5116];
//            int len = -1;
//            while ((len = inputStream.read(buffer)) != -1) {
//                outStream.write(buffer, 0, len);
//            }
//
//            byte[] headerByte = outStream.toByteArray();
//            inputStream.close();
//            outStream.close();
//            parse(headerByte);
//
//            return getTiles(level, query_extent, srcID, in_path, time, measurement);
//
//        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
//            System.out.println("Error occurred: " + e);
//        }
//
//        return null;
//    }
//
//    public static RawTile getTileBuf(RawTile tile) {
//        try {
//            MinioClient minioClient = new MinioClient("http://125.220.153.26:9006", "rssample", "ypfamily608");
//            InputStream inputStream = minioClient.getObject("oge", tile.path, tile.offset[0], tile.offset[1] - tile.offset[0]);
//            int length = Math.toIntExact((tile.offset[1] - tile.offset[0]));
//
//            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
//            byte[] buffer = new byte[length];
//            int len = -1;
//            while ((len = inputStream.read(buffer)) != -1) {
//                outStream.write(buffer, 0, len);
//                Thread.sleep(50);
//            }
//
//            byte[] tilebuffer = outStream.toByteArray();
//            tile.tilebuf = tilebuffer;
//            inputStream.close();
//            outStream.close();
//            return tile;
//        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException | InterruptedException e) {
//            System.out.println("Error occurred: " + e);
//        }
//        return null;
//    }
//
//    private static ArrayList<RawTile> getTiles(int level, double[] query_extent, String crs, String in_path, String time, String measurement) {
//        int l;
//        double resolutionTMS = 0.0;
//        double resolutionOrigin = 0.8559114702712672;
//        if (level == -1) {
//            l = 0;
//        } else {
//            if (level == 0) {
//                resolutionTMS = 156543.033928;
//            }
//            if (level == 1) {
//                resolutionTMS = 78271.516964;
//            }
//            if (level == 2) {
//                resolutionTMS = 39135.758482;
//            }
//            if (level == 3) {
//                resolutionTMS = 19567.879241;
//            }
//            if (level == 4) {
//                resolutionTMS = 9783.939621;
//            }
//            if (level == 5) {
//                resolutionTMS = 4891.969810;
//            }
//            if (level == 6) {
//                resolutionTMS = 2445.984905;
//            }
//            if (level == 7) {
//                resolutionTMS = 1222.992453;
//            }
//            if (level == 8) {
//                resolutionTMS = 611.496226;
//            }
//            if (level == 9) {
//                resolutionTMS = 305.748113;
//            }
//            if (level == 10) {
//                resolutionTMS = 152.874057;
//            }
//            if (level == 11) {
//                resolutionTMS = 76.437028;
//            }
//            if (level == 12) {
//                resolutionTMS = 38.218514;
//            }
//            if (level == 13) {
//                resolutionTMS = 19.109257;
//            }
//            if (level == 14) {
//                resolutionTMS = 9.554629;
//            }
//            if (level == 15) {
//                resolutionTMS = 4.777314;
//            }
//            if (level == 16) {
//                resolutionTMS = 2.388657;
//            }
//            if (level == 17) {
//                resolutionTMS = 1.194329;
//            }
//            if (level == 18) {
//                resolutionTMS = 0.597164;
//            }
//            if (level == 19) {
//                resolutionTMS = 0.298582;
//            }
//            if (level == 20) {
//                resolutionTMS = 0.149291;
//            }
//            l = (int) Math.ceil(Math.log(resolutionTMS / resolutionOrigin) / Math.log(2));
//            System.out.println("l = " + l);
//            if (l > TileOffsets.size() - 1) {
//                throw new RuntimeException("Level is too small!");
//            }
//            if (l < 0) {
//                throw new RuntimeException("Level is too big!");
//            }
//            //if (l != 0) {
//            //    throw new RuntimeException("Level is not origin resolution!");
//            //}
//        }
//
//        double lower_left_long = query_extent[0];
//        double lower_left_lat = query_extent[1];
//        double upper_right_long = query_extent[2];
//        double upper_right_lat = query_extent[3];
//
//        Coordinate pointLower = new Coordinate();
//        Coordinate pointUpper = new Coordinate();
//        pointLower.setX(lower_left_lat);
//        pointLower.setY(lower_left_long);
//        pointUpper.setX(upper_right_lat);
//        pointUpper.setY(upper_right_long);
//        Coordinate pointLowerReprojected = new Coordinate();
//        Coordinate pointUpperReprojected = new Coordinate();
//        CoordinateReferenceSystem crsSource = null;
//        try {
//            crsSource = CRS.decode("EPSG:4326");
//        } catch (FactoryException e) {
//            e.printStackTrace();
//        }
//        CoordinateReferenceSystem crsTarget = null;
//        try {
//            System.out.println("crs = " + crs);
//            crsTarget = CRS.decode(crs);
//        } catch (FactoryException e) {
//            e.printStackTrace();
//        }
//        MathTransform transform = null;
//        try {
//            transform = CRS.findMathTransform(crsSource, crsTarget, true);
//        } catch (FactoryException e) {
//            e.printStackTrace();
//        }
//        try {
//            JTS.transform(pointLower, pointLowerReprojected, transform);
//        } catch (TransformException e) {
//            e.printStackTrace();
//        }
//        try {
//            JTS.transform(pointUpper, pointUpperReprojected, transform);
//        } catch (TransformException e) {
//            e.printStackTrace();
//        }
//        double pmin[] = new double[2];
//        double pmax[] = new double[2];
//        pmin[0] = pointLowerReprojected.getX();
//        pmin[1] = pointLowerReprojected.getY();
//        pmax[0] = pointUpperReprojected.getX();
//        pmax[1] = pointUpperReprojected.getY();
//        // 图像范围
//        // 东西方向空间分辨率  --->像素宽度
//        double w_src = Cell.get(0);
//        // 南北方向空间分辨率 ---> 像素高度
//        double h_src = Cell.get(1);
//        // 左上角x坐标,y坐标 ---> 影像 左上角 投影坐标
//        double xmin = GeoTrans.get(4);
//        double ymax = GeoTrans.get(3);
//
//        ArrayList<RawTile> tile_srch = new ArrayList<>();
//        //计算目标影像的左上和右下图上坐标
//        int p_left = (int) ((pmin[0] - xmin) / (512 * w_src * (int) Math.pow(2, l)));
//        int p_right = (int) ((pmax[0] - xmin) / (512 * w_src * (int) Math.pow(2, l)));
//        int p_lower = (int) ((ymax - pmax[1]) / (512 * h_src * (int) Math.pow(2, l)));
//        int p_upper = (int) ((ymax - pmin[1]) / (512 * h_src * (int) Math.pow(2, l)));
//        for (int i = (p_lower < 0 ? 0 : p_lower);
//             i <= (p_upper >= TileOffsets.get(l).size() / 3 ? TileOffsets.get(l).size() / 3 - 1 : p_upper); i++) {
//            for (int j = (p_left < 0 ? 0 : p_left); j <= (p_right >= TileOffsets.get(l).get(i).size() ? TileOffsets.get(l).get(i).size() - 1 : p_right); j++) {
//                for (int k = 0; k < 3; k++) {
//                    RawTile t1 = new RawTile();
//                    int kPlus = 0;
//                    if (l == 0) {
//                        kPlus = 8;
//                    }
//                    if (l == 1) {
//                        kPlus = 4;
//                    }
//                    if (l == 2) {
//                        kPlus = 2;
//                    }
//                    if (l == 3) {
//                        kPlus = 2;
//                    }
//                    t1.offset[0] = TileOffsets.get(l).get(i + k * kPlus).get(j);
//                    t1.offset[1] = TileByteCounts.get(l).get(i).get(j) + t1.offset[0];
//                    t1.p_bottom_left[0] = i * (512 * w_src * (int) Math.pow(2, l)) + xmin;
//                    t1.p_bottom_left[1] = (j + 1) * (512 * -h_src * (int) Math.pow(2, l)) + ymax;
//                    t1.p_upper_right[0] = (i + 1) * (512 * w_src * (int) Math.pow(2, l)) + xmin;
//                    t1.p_upper_right[1] = j * (512 * -h_src * (int) Math.pow(2, l)) + ymax;
//                    t1.rotation = GeoTrans.get(5);
//                    t1.resolution = w_src * (int) Math.pow(2, l);
//                    t1.row_col[0] = i;
//                    t1.row_col[1] = j;
//                    t1.bitpersample = BitPerSample;
//                    t1.level = l;
//                    t1.path = in_path;
//                    t1.time = time;
//                    t1.measurement = measurement;
//                    tile_srch.add(t1);
//                }
//            }
//        }
//        return tile_srch;
//    }
//
//    private static ArrayList<ArrayList<ArrayList<Integer>>> TileOffsets = new ArrayList<>();
//    private static ArrayList<ArrayList<ArrayList<Integer>>> TileByteCounts = new ArrayList<>();
//    private static byte[] header = new byte[16383];
//    private static String Srid;
//    private static int BitPerSample;
//    private static int ImageWidth;
//    private static int ImageLength;
//    private static ArrayList<Integer> ImageSize = new ArrayList<Integer>();
//    private static ArrayList<Double> Cell = new ArrayList<>();
//    private static ArrayList<Double> GeoTrans = new ArrayList<>();
//    private static final int[] TypeArray = {//"???",
//            0,
//            1,//byte //8-bit unsigned integer
//            1,//ascii//8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero)
//            2, //short",2),//16-bit (2-byte) unsigned integer.
//            4,//  long",4),//32-bit (4-byte) unsigned integer.
//            8,//  rational",8),//Two LONGs: the first represents the numerator of a fraction; the second, the denominator.
//            1,//  sbyte",1),//An 8-bit signed (twos-complement) integer
//            1,//   undefined",1),//An 8-bit byte that may contain anything, depending on the definition of the field
//            2,//   sshort",1),//A 16-bit (2-byte) signed (twos-complement) integer.
//            4,//   slong",1),// A 32-bit (4-byte) signed (twos-complement) integer.
//            8,//    srational",1),//Two SLONG’s: the first represents the numerator of a fraction, the second the denominator.
//            4,//    float",4),//Single precision (4-byte) IEEE format
//            8//    double",8)//Double precision (8-byte) IEEE format
//    };
//
//
//    private static int GetIntII(byte[] pd, int startPos, int Length) {
//        int value = 0;
//        for (int i = 0; i < Length; i++) {
//            value |= pd[startPos + i] << i * 8;
//            if (value < 0) {
//                value += 256 << i * 8;
//            }
//        }
//        return value;
//    }
//
//    private static double GetDouble(byte[] pd, int startPos, int Length) {
//        long value = 0;
//        for (int i = 0; i < Length; i++) {
//            value |= ((long) (pd[startPos + i] & 0xff)) << (8 * i);
//            if (value < 0) {
//                value += (long) 256 << i * 8;
//            }
//        }
//        return Double.longBitsToDouble(value);
//    }
//
//    private static void GetDoubleTrans(int startPos, int typeSize, int count) {
//        for (int i = 0; i < count; i++) {
//            double v = GetDouble(header, (startPos + i * typeSize), typeSize);
//            GeoTrans.add(v);
//        }
//    }
//
//    private static void GetDoubleCell(int startPos, int typeSize, int count) {
//        for (int i = 0; i < count; i++) {
//            double v = GetDouble(header, (startPos + i * typeSize), typeSize);
//            Cell.add(v);
//        }
//    }
//
//    private static String GetString(byte[] pd, int startPos, int Length) {
//        byte[] dd = new byte[Length];
//        System.arraycopy(pd, startPos, dd, 0, Length);
//        String s = new String(dd);
//        return s;
//    }
//
//    private static void GetOffsetArray(int startPos, int typeSize, int count) {
//        ArrayList<ArrayList<Integer>> StripOffsets = new ArrayList<>();
//        for (int k = 0; k < 3; k++) {
//            for (int i = 0; i < (ImageLength / 512) + 1; i++) {
//                ArrayList<Integer> Offsets = new ArrayList<>();
//                for (int j = 0; j < (ImageWidth / 512) + 1; j++) {
//                    int v = GetIntII(header, (startPos + (int) (k * ((ImageWidth / 512) + 1) * ((ImageLength / 512) + 1) + i * ((ImageWidth / 512) + 1) + j) * typeSize), typeSize);
//                    Offsets.add(v);
//                }
//                StripOffsets.add(Offsets);
//            }
//        }
//        TileOffsets.add(StripOffsets);
//    }
//
//    private static void GetTileBytesArray(int startPos, int typeSize, int count) {
//        ArrayList<ArrayList<Integer>> Stripbytes = new ArrayList<>();
//        for (int k = 0; k < 3; k++) {
//            for (int i = 0; i < (ImageLength / 512) + 1; i++) {
//                ArrayList<Integer> Tilebytes = new ArrayList<>();
//                for (int j = 0; j < (ImageWidth / 512) + 1; j++) {
//                    int v = GetIntII(header, (startPos + (int) (k * ((ImageWidth / 512) + 1) * ((ImageLength / 512) + 1) + i * ((ImageWidth / 256) + 1) + j) * typeSize), typeSize);
//                    Tilebytes.add(v);
//                }
//                Stripbytes.add(Tilebytes);
//            }
//        }
//        TileByteCounts.add(Stripbytes);
//    }
//
//    private static int DecodeIFH() {
//        return GetIntII(header, 4, 4);
//    }
//
//    private static int DecodeIFD(int n) {
//        int DECount = GetIntII(header, n, 2);
//        n += 2;
//        for (int i = 0; i < DECount; i++) {
//            DecodeDE(n);
//            n += 12;
//        }
//        int pNext = GetIntII(header, n, 4);
//        return pNext;
//    }
//
//    private static void DecodeDE(int pos) {
//        int TagIndex = GetIntII(header, pos, 2);
//        int TypeIndex = GetIntII(header, pos + 2, 2);
//        int Count = GetIntII(header, pos + 4, 4);
//
//        //先找到数据的位置
//        int pData = pos + 8;
//        int totalSize = TypeArray[TypeIndex] * Count;
//        if (totalSize > 4) {
//            pData = GetIntII(header, pData, 4);
//        }
//
//        //再根据Tag把值读出并存起来
//        GetDEValue(TagIndex, TypeIndex, Count, pData);
//    }
//
//    private static void GetDEValue(int TagIndex, int TypeIndex, int Count, int pdata) {
//        int typesize = TypeArray[TypeIndex];
//        switch (TagIndex) {
//            case 256://ImageWidth
//                ImageWidth = GetIntII(header, pdata, typesize);
//                ImageSize.add(ImageWidth);
//                break;
//            case 257://ImageLength
//                ImageLength = GetIntII(header, pdata, typesize);
//                ImageSize.add(ImageLength);
//                break;
//            case 258://ImageWidth
//                BitPerSample = GetIntII(header, pdata, typesize);
//                break;
//            case 324: //TileOffsets
//                GetOffsetArray(pdata, typesize, Count);
//                break;
//            case 325: //TileByteCounts
//                GetTileBytesArray(pdata, typesize, Count);
//                break;
//            case 33550://  CellWidth
//                GetDoubleCell(pdata, typesize, Count);
//                break;
//            case 33922: //GeoTransform
//                GetDoubleTrans(pdata, typesize, Count);
//                break;
//            case 34737: //Spatial reference
//                Srid = GetString(header, pdata, typesize * Count);
//                break;
//            default:
//                break;
//        }
//    }
//
//    private static void parse(byte[] stream) {
//        header = stream;
//        int pIFD = DecodeIFH();
//        while (pIFD != 0) {
//            pIFD = DecodeIFD(pIFD);
//        }
//    }
//
//    private static String getSrid() {
//        return Srid;
//    }
//
//    private static ArrayList<Integer> getImageSize() {
//        return ImageSize;
//    }
//
//    private static ArrayList<Double> getGeotrans() {
//        return GeoTrans;
//    }
//
//    private static ArrayList<Double> getCell() {
//        return Cell;
//    }
//
//    private static int getBit() {
//        return BitPerSample;
//    }
//
//    public static ArrayList<Double> getBounds(List<ArrayList<Double>> tile_extents) {
//        Coordinate pointLower = new Coordinate();
//        Coordinate pointUpper = new Coordinate();
//        //double ax = 500;
//        //double ay = 500;
//        //double bx = -500;
//        //double by = -500;
//        //int ix = 0;
//        //int iy = 0;
//        //int jx = 0;
//        //int jy = 0;
//        //for(int i = 0; i < tile_extents.size(); i++) {
//        //    if (tile_extents.get(i).get(0) < ax) {
//        //        ax = tile_extents.get(i).get(0);
//        //        ix = i;
//        //    }
//        //    if (tile_extents.get(i).get(1) < ay) {
//        //        ay = tile_extents.get(i).get(1);
//        //        iy = i;
//        //    }
//        //    if (tile_extents.get(i).get(2) > bx) {
//        //        bx = tile_extents.get(i).get(2);
//        //        jx = i;
//        //    }
//        //    if (tile_extents.get(i).get(3) > by) {
//        //        by = tile_extents.get(i).get(3);
//        //        jy = i;
//        //    }
//        //}
//        //pointLower.setX(tile_extents.get(ix).get(0)-15);
//        //pointLower.setY(tile_extents.get(iy).get(1)-15);
//        //pointUpper.setX(tile_extents.get(jx).get(2)+15);
//        //pointUpper.setY(tile_extents.get(jy).get(3)+15);
//        pointLower.setX(tile_extents.get(0).get(0) - 15);
//        pointLower.setY(tile_extents.get(tile_extents.size() - 1).get(1) - 15);
//        pointUpper.setX(tile_extents.get(tile_extents.size() - 1).get(2) + 15);
//        pointUpper.setY(tile_extents.get(0).get(3) + 15);
//        Coordinate pointLowerReprojected = new Coordinate();
//        Coordinate pointUpperReprojected = new Coordinate();
//        CoordinateReferenceSystem crsSource = null;
//        try {
//            crsSource = CRS.decode("EPSG:32649");
//        } catch (FactoryException e) {
//            e.printStackTrace();
//        }
//        CoordinateReferenceSystem crsTarget = null;
//        try {
//            crsTarget = CRS.decode("EPSG:4326");
//        } catch (FactoryException e) {
//            e.printStackTrace();
//        }
//        MathTransform transform = null;
//        try {
//            transform = CRS.findMathTransform(crsSource, crsTarget);
//        } catch (FactoryException e) {
//            e.printStackTrace();
//        }
//        try {
//            JTS.transform(pointLower, pointLowerReprojected, transform);
//        } catch (TransformException e) {
//            e.printStackTrace();
//        }
//        try {
//            JTS.transform(pointUpper, pointUpperReprojected, transform);
//        } catch (TransformException e) {
//            e.printStackTrace();
//        }
//
//        ArrayList<Double> arrayList = new ArrayList();
//        arrayList.add(pointLowerReprojected.x);
//        arrayList.add(pointLowerReprojected.y);
//        arrayList.add(pointUpperReprojected.x);
//        arrayList.add(pointUpperReprojected.y);
//        return arrayList;
//    }
//
//    public static String getpng(ArrayList<RawTile> tile_srch, String path, String path_img) {
//        System.out.println("tile_srch.size() = " + tile_srch.size());
//
//        int cols = (tile_srch.get(tile_srch.size() - 1).row_col[1] - tile_srch.get(0).row_col[1] + 1) * 256;
//        int rows = (tile_srch.get(tile_srch.size() - 1).row_col[0] - tile_srch.get(0).row_col[0] + 1) * 256;
//
//        String dstPath = path;
//        gdal.AllRegister();
//        Driver dr = gdal.GetDriverByName("PNG");
//        Driver dr1 = gdal.GetDriverByName("MEM");
//        Dataset dm = dr1.Create(dstPath, cols, rows, 1, GDT_Byte);
//        //Dataset ds = dr.Create(dstPath,cols, rows, 1, GDT_Byte);
//
//        for (int i = 0; i < tile_srch.size(); i++) {
//            for (int j = tile_srch.get(0).row_col[0]; j <= tile_srch.get(tile_srch.size() - 1).row_col[0]; j++) {
//                if (tile_srch.get(i).row_col[0] == j) {
//                    int yoff = (tile_srch.get(i).row_col[1] - tile_srch.get(0).row_col[1]) * 256;
//                    int xoff = (tile_srch.get(i).row_col[0] - tile_srch.get(0).row_col[0]) * 256;
//                    dm.GetRasterBand(1).WriteRaster(yoff, xoff, 256, 256, 256, 256, GDT_Byte, tile_srch.get(i).tilebuf);
//                    //ds.GetRasterBand(1).WriteRaster(yoff, xoff, 256, 256, 256, 256, GDT_Byte, tile_srch.get(i).tilebuf);
//                }
//            }
//        }
//        Dataset ds = dr.CreateCopy(dstPath, dm);
//        return "success";
//        //return "{\"vector\":[],\"raster\":[{\"url\":\"" + path_img + "\",\"extent\":[" + arrayList.get(0) + "," + arrayList.get(1) + "," + arrayList.get(2) + "," + arrayList.get(3) + "]}]}";
//    }
//}