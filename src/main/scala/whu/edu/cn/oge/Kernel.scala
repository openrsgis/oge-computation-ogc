package whu.edu.cn.oge


import geotrellis.raster.mapalgebra.{focal, local}
import geotrellis.raster.{DoubleArrayTile, IntArrayTile, isData, isNoData}

object Kernel {

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
  def prewitt(axis: String): focal.Kernel = {
    if (axis == "y") {
      focal.Kernel(IntArrayTile(Array[Int](1, 1, 1, 0, 0, 0, -1, -1, -1), 3, 3))
    }
    else {
      focal.Kernel(IntArrayTile(Array[Int](1, 0, -1, 1, 0, -1, 1, 0, -1), 3, 3))
    }
  }

  /**
   * Generates a 3x3 kirsch edge-detection kernel
   *
   * @param axis Specify the direction of the convolution kernel,x/y
   * @return
   */
  def kirsch(axis: String): focal.Kernel = {
    if (axis == "y") {
      focal.Kernel(IntArrayTile(Array[Int](5, 5, 5, -3, 0, -3, -3, -3, -3), 3, 3))
    }
    else {
      focal.Kernel(IntArrayTile(Array[Int](5, -3, -3, 5, 0, -3, 5, -3, -3), 3, 3))
    }

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
                magnitude: Float = 1)
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
             normalize: Boolean = true,
             magnitude: Float = 1)
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
  def compass(magnitude: Float = 1,
              normalize: Boolean = false)
  : focal.Kernel = {
    val matrix: Array[Int] = Array[Int](1, 1, -1, 1, -2, -1, 1, 1, -1)

    focal.Kernel(DoubleArrayTile(
      if (normalize) matrix.map(_.toDouble * magnitude).map(_ / 5)
      else matrix.map(_.toDouble * magnitude),
      3, 3))

  }


  //noinspection DuplicatedCode
  def diamond(radius: Int,
              normalize: Boolean = true,
              magnitude: Float = 1)
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
                normalize: Boolean = false,
                magnitude: Float = 1)
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
               sigma: Float = 1,
               normalize: Boolean = true,
               magnitude: Float = 1)
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



  //  def robert(axis: String): focal.Kernel = {
  //    if (axis == "y") {
  //      focal.Kernel(IntArrayTile(Array[Int](0, -1, 1, 0), 2, 2))
  //    }
  //    else {
  //      focal.Kernel(IntArrayTile(Array[Int](1, 0, 0, -1), 2, 2))
  //    }
  //  }

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
                normalize: Boolean = false,
                magnitude: Float = 1)
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
              normalize: Boolean = true,
              magnitude: Float = 1)
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
           normalize: Boolean = true,
           magnitude: Float = 1)
  : focal.Kernel = {
    val n: Int = radius * 2 + 1
    val matrix = new Array[Double](n * n)
    // 圈正八边形范围，睡一觉想明白了，正方形加菱形 hh
    for (i <- 0 until n; j <- 0 until n) {
      if (i == radius || j == radius) {
        matrix.update(i * n + j, 1.0)
      }
    }

    genKernel(matrix, n, n, normalize, magnitude)

  }


}

