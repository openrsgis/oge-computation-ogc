package whu.edu.cn.util;


/**
 * Z 曲线编码相关的一些转换工具
 * Morton 码
 */
public class ZCurveUtil {
    public static String lonLatToZCurve(double lon, double lat, int zoom) {
        return xyToZCurve(lonLatToXY(lon, lat, zoom), zoom);
    }

    /**
     * 经纬度转瓦片行列编号
     *
     * @param lon  经度
     * @param lat  维度
     * @param zoom 层级
     * @return x y
     */
    public static int[] lonLatToXY(double lon, double lat, int zoom) {
        double n = Math.pow(2, zoom);

        double tileX = ((lon + 180) / 360) * n;
        double tileY = (1 - (Math.log(Math.tan(Math.toRadians(lat)) +
                (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n;
        int[] xy = new int[]{
                (int) Math.floor(tileX),
                (int) Math.floor(tileY)


        };

        System.out.println(xy[0] + "??? " + xy[1]);
        return xy;
    }

    /**
     * 对行列编号进行Z曲线编码
     *
     * @param xy   行列 x y
     * @param zoom 层级
     * @return 编码结果
     */
    public static String xyToZCurve(int[] xy, int zoom) {
        // Calculate the z-order by bit shuffling
        int res = 0;
        for (int i = 0; i < 2; ++i) {
            for (int j = 0; j < zoom; ++j) {

                int mask = 1 << j;
                // Check whether the value in the position is 1
                if ((xy[i] & mask) != 0)
                    // Do bit shuffling
                    res |= 1 << (i + j * 2);
            }
        }
        return String.valueOf(res);
    }


    /**
     * 经度转行
     *
     * @param lon  经度
     * @param zoom 层级
     * @return x
     */
    public static int lon2Tile(double lon, int zoom) {
        return (int) Math.floor((lon + 180) / 360 * Math.pow(2, zoom));
    }

    /**
     * 纬度转列
     *
     * @param lat  纬度
     * @param zoom 层级
     * @return y
     */
    public static int lat2Tile(double lat, int zoom) {
        double n = Math.pow(2, zoom);
        return (int) Math.floor((1 -
                (Math.log(Math.tan(Math.toRadians(lat)) +
                        (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n);
//        return (int) Math.floor(1 - Math.log(Math.tan(lat * Math.PI / 180) +
//                1 / Math.cos(lat * Math.PI / 180)) / Math.PI / 2 * Math.pow(2, zoom));
    }


    /**
     * x 转经度
     * 这里的经纬度都是左上角的
     * @param x    瓦片x编号
     * @param zoom 层级
     * @return 左上角经度
     */
    public static double tile2Lon(int x, int zoom) {
        return (x / Math.pow(2, zoom) * 360 - 180);
    }

    /**
     * y 转纬度
     * 这里的经纬度都是左上角的
     * @param y    瓦片y编号
     * @param zoom 层级
     * @return 左上角纬度
     */
    public static double tile2Lat(int y, int zoom) {
        double n = Math.PI - 2 * Math.PI * y / Math.pow(2, zoom);
        return (180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))));
    }


    /**
     * 根据索引进行z曲线编码
     *
     * @param x    瓦片 x 编号
     * @param y    瓦片 y 编号
     * @param zoom 层级
     * @return 编码结果
     */
    public static String xyToZCurve(int x, int y, int zoom) {
        int[] xy = new int[]{x, y};
        // Calculate the z-order by bit shuffling
        int res = 0;
        for (int i = 0; i < 2; ++i) {
            for (int j = 0; j < zoom; ++j) {

                int mask = 1 << j;
                // Check whether the value in the position is 1
                if ((xy[i] & mask) != 0)
                    // Do bit shuffling
                    res |= 1 << (i + j * 2);
            }
        }
        return String.valueOf(res);
    }

    public static int[] zCurveToXY(String zIndexStr, int zoom) {
        int zIndex = Integer.parseInt(zIndexStr);
        int[] xy = new int[]{0, 0};

        for (int i = 0; i < 2; ++i) {
            for (int j = 0; j < zoom; ++j) {
                // Task 1.1 zorder，修改以下代码
                int mask = 1 << j * 2 + i;
                // Check whether the value in the position is 1
                if ((zIndex & mask) != 0)
                    // Do bit shuffling
                    xy[i] |= 1 << j;
            }
        }
        return xy;

    }

    public static double[] zCurveToLonLat(String zIndexStr, int zoom) {
        int[] xy = zCurveToXY(zIndexStr, zoom);
        return new double[]{tile2Lon(xy[0], zoom), tile2Lat(xy[0], zoom)};
    }

}
