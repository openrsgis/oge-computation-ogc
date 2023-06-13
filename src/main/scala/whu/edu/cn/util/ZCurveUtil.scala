package whu.edu.cn.util


/**
 * Z 曲线编码相关的一些转换工具
 * Morton 码
 */
object ZCurveUtil {
    def lonLatToZCurve(lon: Double, lat: Double, zoom: Int): String = xyToZCurve(lonLatToXY(lon, lat, zoom), zoom)

    /**
     * 经纬度转瓦片行列编号
     *
     * @param lon  经度
     * @param lat  维度
     * @param zoom 层级
     * @return x y
     */
    def lonLatToXY(lon: Double, lat: Double, zoom: Int): Array[Int] = {
        val n: Double = math.pow(2, zoom)
        val tileX: Double = ((lon + 180) / 360) * n
        val tileY: Double = (1 - (math.log(math.tan(math.toRadians(lat)) + (1 / math.cos(math.toRadians(lat)))) / math.Pi)) / 2 * n
        val xy: Array[Int] = Array[Int](math.floor(tileX).toInt, math.floor(tileY).toInt)
        println(xy(0) + "??? " + xy(1))
        xy
    }

    /**
     * 对行列编号进行Z曲线编码
     *
     * @param xy   行列 x y
     * @param zoom 层级
     * @return 编码结果
     */
    def xyToZCurve(xy: Array[Int], zoom: Int): String = { // Calculate the z-order by bit shuffling
        var res = 0
        for (i <- 0 until 2) {
            for (j <- 0 until zoom) {
                val mask: Int = 1 << j
                // Check whether the value in the position is 1
                if ((xy(i) & mask) != 0) { // Do bit shuffling
                    res |= 1 << (i + j * 2)
                }
            }
        }
        String.valueOf(res)
    }

    /**
     * 经度转行
     *
     * @param lon  经度
     * @param zoom 层级
     * @return x
     */
    def lon2Tile(lon: Double, zoom: Int): Int = math.floor((lon + 180) / 360 * math.pow(2, zoom)).toInt

    /**
     * 纬度转列
     *
     * @param lat  纬度
     * @param zoom 层级
     * @return y
     */
    def lat2Tile(lat: Double, zoom: Int): Int = {
        val n: Double = math.pow(2, zoom)
        math.floor((1 - (math.log(math.tan(math.toRadians(lat)) + (1 / math.cos(math.toRadians(lat)))) / math.Pi)) / 2 * n).toInt
        //        return (int) math.floor(1 - math.log(math.tan(lat * math.PI / 180) +
        //                1 / math.cos(lat * math.PI / 180)) / math.PI / 2 * math.pow(2, zoom));
    }

    /**
     * x 转经度
     * 这里的经纬度都是左上角的
     *
     * @param x    瓦片x编号
     * @param zoom 层级
     * @return 左上角经度
     */
    def tile2Lon(x: Int, zoom: Int): Double = x / math.pow(2, zoom) * 360 - 180

    /**
     * y 转纬度
     * 这里的经纬度都是左上角的
     *
     * @param y    瓦片y编号
     * @param zoom 层级
     * @return 左上角纬度
     */
    def tile2Lat(y: Int, zoom: Int): Double = {
        val n: Double = math.Pi - 2 * math.Pi * y / math.pow(2, zoom)
        180 / math.Pi * math.atan(0.5 * (math.exp(n) - math.exp(-n)))
    }

    /**
     * 根据索引进行z曲线编码
     *
     * @param x    瓦片 x 编号
     * @param y    瓦片 y 编号
     * @param zoom 层级
     * @return 编码结果
     */
    def xyToZCurve(x: Int, y: Int, zoom: Int): String = {
        val xy: Array[Int] = Array[Int](x, y)
        var res = 0
        for (i <- 0 until 2) {
            for (j <- 0 until zoom) {
                val mask: Int = 1 << j
                if ((xy(i) & mask) != 0) res |= 1 << (i + j * 2)
            }
        }
        String.valueOf(res)
    }

    def zCurveToXY(zIndexStr: String, zoom: Int): Array[Int] = {
        val zIndex: Int = zIndexStr.toInt
        val xy: Array[Int] = Array[Int](0, 0)
        for (i <- 0 until 2) {
            for (j <- 0 until zoom) { // Task 1.1 zorder，修改以下代码
                val mask: Int = 1 << j * 2 + i
                if ((zIndex & mask) != 0) xy(i) |= 1 << j
            }
        }
        xy
    }

    def zCurveToLonLat(zIndexStr: String, zoom: Int): Array[Double] = {
        val xy: Array[Int] = zCurveToXY(zIndexStr, zoom)
        Array[Double](tile2Lon(xy(0), zoom), tile2Lat(xy(0), zoom))
    }

}
