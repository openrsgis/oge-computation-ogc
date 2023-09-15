package whu.edu.cn.oge


import geotrellis.raster.mapalgebra.{focal, local}
import geotrellis.raster.{DoubleArrayTile, IntArrayTile, isData, isNoData}

object Kernel {

  /** 矩阵数组 行 列 是否归一化 缩放参数 */
  val genKernel: (Array[Double], Int, Int, Boolean, Float) => focal.Kernel =
    (matrix: Array[Double], rows: Int, cols: Int, normalize: Boolean, magnitude: Float) => {
      val sum: Double = matrix.filter(t => t > 0).sum
      focal.Kernel(DoubleArrayTile(
        if (normalize) matrix.map(_ / sum).map(_ * magnitude)
        else matrix.map(_ * magnitude),
        cols, rows))
    }


  /**
   * Creates a Kernel
   *
   * @param weights A 2-D list to use as the weights of the kernel
   * @return
   */
  def fixed(weights: String): focal.Kernel = {
    val subKernel = weights.substring(2, weights.length - 2).split("],\\[").map(t => {
      t.split(",")
    })
    val col = subKernel.length
    val row = subKernel(0).length
    val kernelArray = subKernel.flatten.map(t => {
      t.toDouble
    })
    focal.Kernel(DoubleArrayTile(kernelArray, col, row))
  }

  /**
   * Generates a square-shaped boolean kernel
   *
   * @param radius    The radius of the kernel to generate.
   * @param normalize Normalize the kernel values to sum to 1
   * @param value     Scale each value by this amount
   * @return
   */
  def square(radius: Int, normalize: Boolean, value: Double): focal.Kernel = {
    if (normalize) {
      val kernelArray = Array.fill[Double]((2 * radius + 1) * (2 * radius + 1))(1.0 / ((2 * radius + 1) * (2 * radius + 1)))
      focal.Kernel(DoubleArrayTile(kernelArray, 2 * radius + 1, 2 * radius + 1))
    }
    else {
      val kernelArray = Array.fill[Double]((2 * radius + 1) * (2 * radius + 1))(value)
      focal.Kernel(DoubleArrayTile(kernelArray, 2 * radius + 1, 2 * radius + 1))

    }
  }

  /**
   * Generates a 3x3 Prewitt edge-detection kernel
   *
   * @param axis Specify the direction of the convolution kernel,x/y
   * @return
   */
  def prewitt(normalize: Boolean = false,
              magnitude: Float): focal.Kernel = {
    genKernel(Array[Int](1, 0, -1, 1, 0, -1, 1, 0, -1).map(_.toDouble), 3, 3, normalize, magnitude)
  }

  /**
   * Generates a 3x3 kirsch edge-detection kernel
   *
   * @param axis Specify the direction of the convolution kernel,x/y
   * @return
   */
  def kirsch(normalize: Boolean = false,
             magnitude: Float): focal.Kernel = {
    val n: Int = 3

    val matrix = Array[Int](5, 5, 5, -3, 0, -3, -3, -3, -3)


    genKernel(matrix.map(_.toDouble), n, n, normalize, magnitude)
  }

  /**
   * Generates a 3x3 sobel edge-detection kernel
   *
   * @param axis Specify the direction of the convolution kernel,x/y
   * @return
   */
  def sobel(axis: String): focal.Kernel = {
    axis match {
      case "y" =>
        focal.Kernel(IntArrayTile(Array[Int](1, 2, 1, 0, 0, 0, -1, -2, -1), 3, 3))
      case "x" =>
        focal.Kernel(IntArrayTile(Array[Int](-1, 0, 1, -2, 0, 2, -1, 0, 1), 3, 3))
      case _ =>
        throw new IllegalArgumentException("expect 'x' or 'y' !")

    }
  }

  def plain(axis: String): focal.Kernel = {
    focal.Kernel(IntArrayTile(Array[Int](1, 1, 1, 1, 0, 1, 1, 1, 1), 3, 3))
  }


  //noinspection DuplicatedCode
  def chebyshev(radius: Int,
                normalize: Boolean = false,
                magnitude: Float)
  : focal.Kernel = {

    val n: Int = radius * 2 + 1
    val matrix = new Array[Int](n * n)

    // 根据到中心的距离计算每个元素的值
    for (i <- 0 until n; j <- 0 until n) {
      val distance: Int = math.max(math.abs(i - radius), math.abs(j - radius))
      matrix.update(i * n + j, distance)
    }


    genKernel(matrix.map(_.toDouble), n, n, normalize, magnitude)

  }

  //noinspection DuplicatedCode
  def circle(radius: Int,
             normalize: Boolean,
             magnitude: Float)
  : focal.Kernel = {
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    // 确定位置
    for (i <- 0 until n; j <- 0 until n) {
      val distance: Double = math.sqrt(
        (i - radius) * (i - radius) + (j - radius) * (j - radius)
      )
      if (distance <= radius) {
        matrix.update(i * n + j, 1.0)
      }
    }

    genKernel(matrix, n, n, normalize, magnitude)

  }


  //noinspection DuplicatedCode
  def compass(normalize: Boolean = false, magnitude: Float)
  : focal.Kernel = {
    val matrix: Array[Int] = Array[Int](1, 1, -1, 1, -2, -1, 1, 1, -1)

    focal.Kernel(DoubleArrayTile(
      if (normalize) matrix.map(_.toDouble * magnitude).map(_ / 5)
      else matrix.map(_.toDouble * magnitude),
      3, 3))

  }


  //noinspection DuplicatedCode
  def diamond(radius: Int,
              normalize: Boolean,
              magnitude: Float)
  : focal.Kernel = {

    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    // 确定位置
    for (i <- 0 until n; j <- 0 until n) {
      val distance: Double = math.abs(i - radius) + math.abs(j - radius)
      if (distance <= radius) {
        matrix.update(i * n + j, 1.0)
      }
    }

    genKernel(matrix, n, n, normalize, magnitude)
    //    focal.Kernel(DoubleArrayTile(matrix.map(_ / sum), n, n))
  }

  //noinspection DuplicatedCode
  def euclidean(radius: Int,
                normalize: Boolean,
                magnitude: Float)
  : focal.Kernel = {
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    // 计算欧氏距离
    for (i <- 0 until n; j <- 0 until n) {
      val distance: Double = math.sqrt(
        (i - radius) * (i - radius) + (j - radius) * (j - radius)
      )
      matrix.update(i * n + j, distance)
    }

    genKernel(matrix, n, n, normalize, magnitude)

  }


  //noinspection DuplicatedCode
  def gaussian(radius: Int,
               sigma: Float,
               normalize: Boolean,
               magnitude: Float)
  : focal.Kernel = {
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    // 计算高斯分布
    for (i <- 0 until n; j <- 0 until n) {
      val squareDistance: Double =
        (i - radius) * (i - radius) + (j - radius) * (j - radius)

      matrix.update(i * n + j,
        1.0 / (2 * math.Pi * sigma * sigma)
          * math.exp(-squareDistance / (2 * sigma * sigma))
      )
    }

    genKernel(matrix, n, n, normalize, magnitude)


  }

  def inverse(kernel: focal.Kernel): focal.Kernel = {
    focal.Kernel(
      kernel.tile.mapDouble(t => {
        if (isNoData(t) || t.equals(0.0)) t
        else 1.0 / t
      })
    )
  }


  /**
   * Generates a 3x3 laplacian-4 edge-detection kernel
   *
   * @return
   */
  def laplacian4(): focal.Kernel = {
    focal.Kernel(IntArrayTile(Array[Int](0, 1, 0, 1, -4, 1, 0, 1, 0), 3, 3))
  }

  /**
   * Generates a 3x3 laplacian-8 edge-detection kernel
   *
   * @return
   */
  def laplacian8(): focal.Kernel = {
    focal.Kernel(IntArrayTile(Array[Int](1, 1, 1, 1, -8, 1, 1, 1, 1), 3, 3))
  }

  /**
   * Adds two kernels
   *
   * @param kernel1 The first kernel
   * @param kernel2 The second kernel
   * @return
   */
  def add(kernel1: focal.Kernel, kernel2: focal.Kernel): focal.Kernel = {
    focal.Kernel(local.Add(kernel1.tile, kernel2.tile))
  }

  //noinspection DuplicatedCode
  def manhattan(radius: Int,
                normalize: Boolean,
                magnitude: Float)
  : focal.Kernel = {
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    // 计算曼哈顿分布
    for (i <- 0 until n; j <- 0 until n) {
      val distance: Double = math.abs(i - radius) + math.abs(j - radius)

      matrix.update(i * n + j, distance)
    }

    genKernel(matrix, n, n, normalize, magnitude)

  }


  //noinspection DuplicatedCode
  def octagon(radius: Int,
              normalize: Boolean,
              magnitude: Float)
  : focal.Kernel = {
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    // 圈正八边形范围，睡一觉想明白了，正方形加菱形 hh
    for (i <- 0 until n; j <- 0 until n) {
      val distance: Double = math.abs(i - radius) + math.abs(j - radius)

      if (distance <= math.sqrt(2) * radius) {
        matrix.update(i * n + j, 1.0)
      }
    }

    genKernel(matrix, n, n, normalize, magnitude)

  }

  //noinspection DuplicatedCode
  def plus(radius: Int,
           normalize: Boolean,
           magnitude: Float)
  : focal.Kernel = {
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    for (i <- 0 until n; j <- 0 until n) {
      if (i == radius || j == radius) {
        matrix.update(i * n + j, 1.0)
      }
    }

    genKernel(matrix, n, n, normalize, magnitude)

  }

  //noinspection DuplicatedCode
  def rectangle(xRadius: Int,
                yRadius: Int,
                normalize: Boolean,
                magnitude: Float)
  : focal.Kernel = {
    val rows: Int = yRadius * 2 + 1
    val cols: Int = xRadius * 2 + 1
    val matrix = new Array[Double](rows * cols)

    genKernel(matrix.map(_ => 1.0), rows, cols, normalize, magnitude)

  }


  def roberts(normalize: Boolean, magnitude: Float)
  : focal.Kernel = {

    genKernel(Array[Double](1, 0, 0, -1), 2, 2, normalize, magnitude)

  }

  /**
   * Rotate the kernel according to the rotations.
   *
   * @param kernel    The kernel to be rotated.
   * @param rotations Number of 90 deg.
   *                  rotations to make (negative numbers rotate counterclockwise).
   * @return
   */
  //noinspection DuplicatedCode
  def rotate(kernel: focal.Kernel,
             rotations: Int)
  : focal.Kernel = {

    // 必须行列相等
    if (kernel.tile.rows != kernel.tile.cols) {
      throw new IllegalArgumentException("kernel 's rows must be equal to its cols!")
    }

    // 准备条件
    val n: Int = kernel.tile.rows
    val data: Array[Double] = kernel.tile.toArrayDouble()
    val radius: Int = (n - 1) >> 1
    val A: Double = -rotations * math.Pi / 2

    val matrix = new Array[Double](n * n)


    for (i <- 0 until n; j <- 0 until n) {
      val newI: Int = // 四舍五入！
        (-(j - radius) * math.sin(A) + (i - radius) * math.cos(A) + radius + 0.5).toInt
      val newJ: Int = // 四舍五入！
        ((j - radius) * math.cos(A) + (i - radius) * math.sin(A) + radius + 0.5).toInt
      matrix.update(newI * n + newJ, data(i * n + j))
    }

    focal.Kernel(DoubleArrayTile(matrix, n, n))

  }

  def main(args: Array[String]): Unit = {
    val radius = 2
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    for (i <- 0 until n; j <- 0 until n) {
      if (i == radius || j == radius) {
        matrix.update(i * n + j, 1.0)
      }
    }
    matrix.foreach(d => {
      print(d, "")
    })
  }


}

