//package whu.edu.cn.algorithms.gmrc.colorbalance
//
//import java.awt.image.BufferedImage
//import java.io.File
//import javax.imageio.ImageIO
//
//object ImageReader {
//  def main(args: Array[String]): Unit = {
//    val image: BufferedImage = ImageIO.read(new File("path/to/your/image.jpg"))
//
//    val width = image.getWidth
//    val height = image.getHeight
//
//    for (x <- 0 until width) {
//      for (y <- 0 until height) {
//        val pixel = image.getRGB(x, y)
//        // 对于每个像素，处理颜色值
//        // 例如，提取红色、绿色和蓝色分量
//        val red = (pixel >> 16) & 0xff
//        val green = (pixel >> 8) & 0xff
//        val blue = pixel & 0xff
//
//        // 这里可以添加你的代码来处理每个像素
//      }
//    }
//  }
//}
//
//class colorbalance {
//
//  private val m_si_file: File = null
//  private def readImgToBlocks(imgFile: File, blockDim: Int): Vector[Vector[Short]] = {
//    val image: BufferedImage = ImageIO.read(imgFile)
//
//    val width = image.getWidth
//    val height = image.getHeight
//
//    for (x <- 0 until width) {
//      for (y <- 0 until height) {
//        val pixel = image.getRGB(x, y)
//        // 对于每个像素，处理颜色值
//        // 例如，提取红色、绿色和蓝色分量
//        val red = (pixel >> 16) & 0xff
//        val green = (pixel >> 8) & 0xff
//        val blue = pixel & 0xff
//
//        // 这里可以添加你的代码来处理每个像素
//      }
//    }
//  }
//
//}
//
//
//
//
//
//
//
//
//
//
