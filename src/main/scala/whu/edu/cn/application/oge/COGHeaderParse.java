package whu.edu.cn.application.oge;


import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.errors.*;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.algorithm.MinimumDiameter;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.ProjectedCRS;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;


import static whu.edu.cn.util.SystemConstants.*;


public class COGHeaderParse {
    public static int tileDifference = 0;
    private static final int[] TypeArray = {//"???",
            0,//
            1,// byte //8-bit unsigned integer
            1,// ascii//8-bit byte that contains a 7-bit ASCII code; the last byte must be NUL (binary zero)
            2,// short",2),//16-bit (2-byte) unsigned integer.
            4,// long",4),//32-bit (4-byte) unsigned integer.
            8,// rational",8),//Two LONGs: the first represents the numerator of a fraction; the second, the denominator.
            1,// sbyte",1),//An 8-bit signed (twos-complement) integer
            1,// undefined",1),//An 8-bit byte that may contain anything, depending on the definition of the field
            2,// sshort",1),//A 16-bit (2-byte) signed (twos-complement) integer.
            4,// slong",1),// A 32-bit (4-byte) signed (twos-complement) integer.
            8,// srational",1),//Two SLONG’s: the first represents the numerator of a fraction, the second the denominator.
            4,// float",4),//Single precision (4-byte) IEEE format
            8 // double",8)//Double precision (8-byte) IEEE format
    };

    /**
     * 根据元数据查询 tile
     *
     * @param level         JSON中的level字段，前端层级
     * @param in_path       获取tile的路径
     * @param time
     * @param crs
     * @param measurement   影像的测量方式
     * @param dType
     * @param resolution
     * @param productName
     * @param queryGeometry 查询瓦片的矩形范围
     * @param bandCounts    多波段
     * @return 后端瓦片
     */
    public static ArrayList<RawTile> tileQuery(MinioClient minioClient,
                                               int level, String in_path,
                                               String time, String crs, String measurement,
                                               String dType, String resolution, String productName,
                                               Geometry queryGeometry,
                                               int... bandCounts) throws IOException {
        int bandCount = 1;
        if (bandCounts.length > 1) {
            throw new RuntimeException("bandCount 参数最多传一个");
        }
        if (bandCounts.length == 1) {
            bandCount = bandCounts[0];
        }

        final int[] imageSize = new int[]{0, 0}; // imageLength & imageWidth

        final ArrayList<ArrayList<ArrayList<Integer>>> tileByteCounts = new ArrayList<>();
        final ArrayList<Double> geoTrans = new ArrayList<>();//TODO 经纬度
        final ArrayList<Double> cell = new ArrayList<>();
        final ArrayList<ArrayList<ArrayList<Integer>>> tileOffsets = new ArrayList<>();

        try {
            // 获取指定offset和length的"myobject"的输入流。
            InputStream inputStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket("oge")
                            .object(in_path)
                            .offset(0L)
                            .length((long) HEAD_SIZE).build()
            );

            // Read data from stream
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[HEAD_SIZE];//16383
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, len);
            }

            byte[] headerByte = outStream.toByteArray();
            outStream.close();
            inputStream.close();
            parse(headerByte, tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount);
            System.out.println(cell);

            return getTiles(level, queryGeometry, crs, in_path, time, measurement, dType, resolution, productName, tileOffsets, cell, geoTrans, tileByteCounts, bandCount);


        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            System.out.println("Error occurred: " + e);
        }

        return null;
    }


    /**
     * 获取 tile 影像本体
     *
     * @param tile tile相关数据
     * @return
     */
    public static RawTile getTileBuf(MinioClient minioClient, RawTile tile) {
        try {
            int length = Math.toIntExact(tile.getOffset()[1]);
            InputStream inputStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket("oge")
                            .object(tile.getPath())
                            .offset(tile.getOffset()[0])
                            .length((long) length).build()
            );

            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[length];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, len);
            }
            tile.setTileBuf(outStream.toByteArray());
            inputStream.close();
            outStream.close();
            return tile;
        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            System.out.println("Error occurred: " + e);
        }
        return null;
    }

    /**
     * 获取 Tile 相关的一些数据，不包含tile影像本体
     *
     * @param level         json里的 level 字段，表征前端 Zoom
     * @param queryGeometry
     * @param crs
     * @param in_path
     * @param time
     * @param measurement
     * @param dType
     * @param resolution    数据库中的原始影像分辨率
     * @param productName
     * @return
     */
    private static ArrayList<RawTile> getTiles(int level, Geometry queryGeometry,
                                               String crs, String in_path, String time,
                                               String measurement, String dType,
                                               String resolution, String productName,
                                               final ArrayList<ArrayList<ArrayList<Integer>>> tileOffsets,
                                               final ArrayList<Double> cell,
                                               final ArrayList<Double> geoTrans,
                                               final ArrayList<ArrayList<ArrayList<Integer>>> tileByteCounts,
                                               int bandCount) {

        int tileLevel;
        double resolutionTMS;
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
        if (level == -1) {
            tileLevel = 0;
        } else {
            resolutionTMS = resolutionTMSArray[level];
            System.out.println("level = " + level);
            tileLevel = (int) Math.floor(Math.log(resolutionTMS / resolutionOrigin) / Math.log(2)) + 1;
            System.out.println("tileLevel = " + tileLevel);
            if (tileLevel > tileOffsets.size()) {
                tileLevel = tileOffsets.size() - 1;
                tileDifference = tileLevel - (tileOffsets.size() - 1);
            } else if (tileLevel < 0) {
                tileLevel = 0;
                tileDifference = tileLevel;
            }
        }

        MinimumDiameter minimumDiameter = new MinimumDiameter(queryGeometry);
        Geometry minimumRectangle = minimumDiameter.getMinimumRectangle();
        Envelope envelope = minimumRectangle.getEnvelopeInternal();
        double lower_left_long = envelope.getMinX();
        double lower_left_lat = envelope.getMinY();
        double upper_right_long = envelope.getMaxX();
        double upper_right_lat = envelope.getMaxY();

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
        double w_src = cell.get(0);
        // 南北方向空间分辨率 ---> 像素高度 // TODO
        double h_src = cell.get(1);
        // 左上角x坐标,y坐标 ---> 影像 左上角 投影坐标
        double xMin;
        double yMax;

        if (flag) {
            xMin = geoTrans.get(3);
            yMax = geoTrans.get(4);
        } else {
            xMin = geoTrans.get(4);
            yMax = geoTrans.get(3);
        }

        ArrayList<RawTile> tile_srch = new ArrayList<>();

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

        //计算目标影像的左上和右下图上坐标
        int p_left;
        int p_right;
        int p_lower;
        int p_upper;
        if (flag) {
            p_left = (int) ((pMin[0] - xMin) / (256 * w_src * (int) Math.pow(2, tileLevel)));
            p_right = (int) ((pMax[0] - xMin) / (256 * w_src * (int) Math.pow(2, tileLevel)));
            p_lower = (int) ((yMax - pMax[1]) / (256 * h_src * (int) Math.pow(2, tileLevel)));
            p_upper = (int) ((yMax - pMin[1]) / (256 * h_src * (int) Math.pow(2, tileLevel)));
        } else {
            p_lower = (int) ((pMin[1] - yMax) / (256 * w_src * (int) Math.pow(2, tileLevel)));
            p_upper = (int) ((pMax[1] - yMax) / (256 * w_src * (int) Math.pow(2, tileLevel)));
            p_left = (int) ((xMin - pMax[0]) / (256 * h_src * (int) Math.pow(2, tileLevel)));
            p_right = (int) ((xMin - pMin[0]) / (256 * h_src * (int) Math.pow(2, tileLevel)));
        }

        int[] pCoordinate;
        double[] srcSize;
        double[] pRange;
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
             i <= (pCoordinate[1] >= tileOffsets.get(tileLevel).size() / bandCount ?
                     tileOffsets.get(tileLevel).size() / bandCount - 1 : pCoordinate[1]);
             i++
        ) {
            for (int j = (Math.max(pCoordinate[2], 0));
                 j <= (pCoordinate[3] >= tileOffsets.get(tileLevel).get(i).size() ?
                         tileOffsets.get(tileLevel).get(i).size() - 1 : pCoordinate[3]);
                 j++) {
                for (int k = 0; k < bandCount; k++) {
                    Coordinate minCoordinate = new Coordinate(j * (256 * srcSize[0] * Math.pow(2, tileLevel)) + pRange[0], (i + 1) * (256 * -srcSize[1] * Math.pow(2, tileLevel)) + pRange[1]); // 矩形左下角经纬度
                    Coordinate maxCoordinate = new Coordinate((j + 1) * (256 * srcSize[0] * Math.pow(2, tileLevel)) + pRange[0], i * (256 * -srcSize[1] * Math.pow(2, tileLevel)) + pRange[1]); // 矩形右上角经纬度
                    Coordinate[] coordinates = new Coordinate[]{
                            minCoordinate,
                            new Coordinate(maxCoordinate.x, minCoordinate.y),
                            maxCoordinate,
                            new Coordinate(minCoordinate.x, maxCoordinate.y),
                            minCoordinate
                    };
                    Polygon tilePolygon = new GeometryFactory().createPolygon(new CoordinateArraySequence(coordinates));
                    if (tilePolygon.intersects(queryGeometry)) {
                        RawTile t = new RawTile();
                        t.setOffset(tileOffsets.get(tileLevel).get(i).get(j),
                                tileByteCounts.get(tileLevel).get(i).get(j) + t.getOffset()[0]);
                        t.setLngLatBottomLeft(
                                j * (256 * srcSize[0] * Math.pow(2, tileLevel)) + pRange[0],
                                (i + 1) * (256 * -srcSize[1] * Math.pow(2, tileLevel)) + pRange[1]
                        );
                        t.setLngLatUpperRight(
                                (j + 1) * (256 * srcSize[0] * Math.pow(2, tileLevel)) + pRange[0],
                                i * (256 * -srcSize[1] * Math.pow(2, tileLevel)) + pRange[1]
                        );
                        t.setRotation(geoTrans.get(5));
                        t.setResolution(w_src * Math.pow(2, tileLevel));
                        t.setRowCol(i, j);
                        t.setLevel(tileLevel);
                        t.setPath(in_path);
                        t.setTime(time);
                        if (bandCount == 1) {
                            t.setMeasurement(measurement);
                        } else {
                            t.setMeasurement(String.valueOf(bandCount)); //TODO
                        }
                        t.setCrs(Integer.parseInt(crs.replace("EPSG:", "")));
                        t.setDataType(dType);
                        t.setProduct(productName);
                        tile_srch.add(t);
                    }
                }
            }
        }


        return tile_srch;
    }


    /**
     * 解析参数，并修改一些数据
     *
     * @param header
     * @param tileOffsets
     * @param cell
     * @param geoTrans
     * @param tileByteCounts
     * @param imageSize      图像尺寸
     */
    private static void parse(byte[] header,
                              final ArrayList<ArrayList<ArrayList<Integer>>> tileOffsets,
                              final ArrayList<Double> cell,
                              final ArrayList<Double> geoTrans,
                              final ArrayList<ArrayList<ArrayList<Integer>>> tileByteCounts,
                              final int[] imageSize,
                              int bandCount) {

        int pIFD = getIntII(header, 4, 4);       // DecodeIFH

        // DecodeIFD
        while (pIFD != 0) {
            int DECount = getIntII(header, pIFD, 2);
            pIFD += 2;
            for (int i = 0; i < DECount; i++) {  // DecodeDE

                int TagIndex = getIntII(header, pIFD, 2);
                int TypeIndex = getIntII(header, pIFD + 2, 2);
                int Count = getIntII(header, pIFD + 4, 4);

                //先找到数据的位置
                int pData = pIFD + 8;
                int totalSize = TypeArray[TypeIndex] * Count;
                if (totalSize > 4) {
                    pData = getIntII(header, pData, 4);
                }

                //再根据Tag把值读出并存起来，GetDEValue
                getDEValue(TagIndex, TypeIndex, Count, pData, header,
                        tileOffsets, cell, geoTrans, tileByteCounts, imageSize, bandCount);

                // 之前的
                pIFD += 12;

            } // end for
            pIFD = getIntII(header, pIFD, 4);

        } // end while
    }


    private static void getDEValue(int tagIndex, int typeIndex, int count, int pData, byte[] header,
                                   final ArrayList<ArrayList<ArrayList<Integer>>> tileOffsets,
                                   final ArrayList<Double> cell,
                                   final ArrayList<Double> geoTrans,
                                   final ArrayList<ArrayList<ArrayList<Integer>>> tileByteCounts,
                                   final int[] imageSize,
                                   int bandCount) {
        int typeSize = TypeArray[typeIndex];
        switch (tagIndex) {
            case 256://ImageWidth
                imageSize[1] = getIntII(header, pData, typeSize);
                break;
            case 257://ImageLength
                imageSize[0] = getIntII(header, pData, typeSize);
                break;
            case 258://ImageWidth
                int BitPerSample = getIntII(header, pData, typeSize);
                break;
            case 286://XPosition
                int xPosition = getIntII(header, pData, typeSize);
                break;
            case 287://YPosition
                int yPosition = getIntII(header, pData, typeSize);
                break;
            case 324: //tileOffsets
                getOffsetArray(pData, typeSize, header, tileOffsets, imageSize, bandCount);
                break;
            case 325: //tileByteCounts
                getTileBytesArray(pData, typeSize, header, tileByteCounts, imageSize, bandCount);
                break;
            case 33550://  cellWidth
                getDoubleCell(pData, typeSize, count, header, cell);
                break;
            case 33922: //geoTransform
                getDoubleTrans(pData, typeSize, count, header, geoTrans);
                break;
            case 34737: //Spatial reference
                String crs = getString(header, pData, typeSize * count);
                break;
            default:
                break;
        }
    }


    private static int getIntII(byte[] pd, int startPos, int Length) {
        int value = 0;
        for (int i = 0; i < Length; i++) {
            value |= pd[startPos + i] << i * 8;
            if (value < 0) {
                value += 256 << i * 8;
            }
        }
        return value;
    }

    private static double getDouble(byte[] pd, int startPos, int Length) {
        long value = 0;
        for (int i = 0; i < Length; i++) {
            value |= ((long) (pd[startPos + i] & 0xff)) << (8 * i);
            if (value < 0) {
                value += (long) 256 << i * 8;
            }
        }
        return Double.longBitsToDouble(value);
    }


    private static void getDoubleTrans(int startPos, int typeSize, int count, byte[] header,
                                       final ArrayList<Double> geoTrans) {
        for (int i = 0; i < count; i++) {
            double v = getDouble(header, (startPos + i * typeSize), typeSize);
            geoTrans.add(v);
        }
    }


    private static void getDoubleCell(int startPos, int typeSize, int count, byte[] header,
                                      final ArrayList<Double> cell) {
        for (int i = 0; i < count; i++) {
            double v = getDouble(header, (startPos + i * typeSize), typeSize);
            cell.add(v);
        }
    }


    private static String getString(byte[] pd, int startPos, int Length) {
        byte[] dd = new byte[Length];
        System.arraycopy(pd, startPos, dd, 0, Length);
        return new String(dd);
    }


    private static void getOffsetArray(int startPos, int typeSize, byte[] header,
                                       final ArrayList<ArrayList<ArrayList<Integer>>> tileOffsets,
                                       final int[] imageSize, int bandCount) {

        ArrayList<ArrayList<Integer>> StripOffsets = new ArrayList<>();

        for (int k = 0; k < bandCount; k++) {
            for (int i = 0; i < (imageSize[0] / 256) + 1; i++) {
                ArrayList<Integer> Offsets = new ArrayList<>();
                for (int j = 0; j < (imageSize[1] / 256) + 1; j++) {
                    int v = getIntII(header, (startPos +
                            (i * ((imageSize[1] / 256) + 1) + j) * typeSize), typeSize);
                    Offsets.add(v);
                }
                StripOffsets.add(Offsets);
            }
        }
        tileOffsets.add(StripOffsets);
    }

    private static void getTileBytesArray(int startPos, int typeSize, byte[] header,
                                          final ArrayList<ArrayList<ArrayList<Integer>>> tileByteCounts,
                                          final int[] imageSize, int bandCount) {
        ArrayList<ArrayList<Integer>> stripBytes = new ArrayList<>();

        for (int k = 0; k < bandCount; k++) {
            for (int i = 0; i < (imageSize[0] / 256) + 1; i++) {
                ArrayList<Integer> tileBytes = new ArrayList<>();
                for (int j = 0; j < (imageSize[1] / 256) + 1; j++) {
                    int v = getIntII(header, (startPos +
                            (i * ((imageSize[1] / 256) + 1) + j) * typeSize), typeSize);
                    tileBytes.add(v);
                }
                stripBytes.add(tileBytes);
            }
        }
        tileByteCounts.add(stripBytes);
    }

}



