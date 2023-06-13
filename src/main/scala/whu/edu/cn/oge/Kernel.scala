package whu.edu.cn.oge

import geotrellis.raster.mapalgebra.focal._
import geotrellis.raster.mapalgebra.{focal, local}
import geotrellis.raster.{DoubleArrayTile, IntArrayTile}

object Kernel {
  /**
   * Creates a Kernel
   *
   * @param weights A 2-D list to use as the weights of the kernel
   * @return
   */
  def fixed(weights: String): Kernel = {
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
   * @param radius The radius of the kernel to generate.
   * @param normalize Normalize the kernel values to sum to 1
   * @param value Scale each value by this amount
   * @return
   */
  def square(radius: Int, normalize: Boolean, value: Double): Kernel = {
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
  def prewitt(axis: String): Kernel = {
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
  def kirsch(axis: String): Kernel = {
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
  def sobel(axis: String): Kernel = {
    if (axis == "y") {
      focal.Kernel(IntArrayTile(Array[Int](1, 2, 1, 0, 0, 0, -1, -2, -1), 3, 3))
    }
    else {
      focal.Kernel(IntArrayTile(Array[Int](-1, 0, 1, -2, 0, 2, -1, 0, 1), 3, 3))
    }
  }

//  def robert(axis: String): Kernel = {
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
  def laplacian4(): Kernel = {
    focal.Kernel(IntArrayTile(Array[Int](0, 1, 0, 1, -4, 1, 0, 1, 0), 3, 3))
  }

  /**
   * Generates a 3x3 laplacian-8 edge-detection kernel
   *
   * @return
   */
  def laplacian8(): Kernel = {
    focal.Kernel(IntArrayTile(Array[Int](1, 1, 1, 1, -8, 1, 1, 1, 1), 3, 3))
  }

  /**
   * Adds two kernels
   *
   * @param kernel1 The first kernel
   * @param kernel2 The second kernel
   * @return
   */
  def add(kernel1: Kernel, kernel2: Kernel): Kernel = {
    focal.Kernel(local.Add(kernel1.tile, kernel2.tile))
  }
}

