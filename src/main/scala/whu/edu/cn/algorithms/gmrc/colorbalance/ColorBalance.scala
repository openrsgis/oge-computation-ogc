package whu.edu.cn.algorithms.gmrc.colorbalance

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

object ColorBalance {
  def main(args: Array[String]): Unit = {
    val image: BufferedImage = ImageIO.read(new File("path/to/your/image.jpg"))

    val width = image.getWidth
    val height = image.getHeight

    for (x <- 0 until width) {
      for (y <- 0 until height) {
        val pixel = image.getRGB(x, y)
        // 对于每个像素，处理颜色值
        // 例如，提取红色、绿色和蓝色分量
        val red = (pixel >> 16) & 0xff
        val green = (pixel >> 8) & 0xff
        val blue = pixel & 0xff

        // 这里可以添加你的代码来处理每个像素
      }
    }
  }
}

class ColorBalance {

  type ImageDataset = BufferedImage

  private val m_si_file: File = null

  /**
   * 按照 LRM 思想，将图像分块
   * @param inputImgPath  输入的图像路径
   * @param horBlocks  图像水平方向分块数，即为行数
   * @param verBlocks  图像垂直方向分块数，即为列数
   * @return
   */
  private def splitImg(inputImgPath: String, horBlocks: Int, verBlocks: Int): Array[ImageDataset] = {
    // 读取输入图像
    val inputImage = ImageIO.read(new File(inputImgPath))

    // 获取输入图像的宽度和高度
    val inputImgWidth = inputImage.getWidth
    val inputImgHeight = inputImage.getHeight

    // 为最后一块考虑
    val regularHorBlocks = horBlocks - 1
    val regularVerBlocks = verBlocks - 1

    // 计算水平和垂直高度
    val regularImgWidth = inputImgWidth / regularHorBlocks
    val regularImgHeight = inputImgHeight / regularVerBlocks

    // 计算剩余的一个可能块的水平和垂直高度
    val leftImgWidth = inputImgWidth % regularHorBlocks
    val leftImgHeight = inputImgHeight % regularVerBlocks

    // 创建输出数组
    val subImgBlockArr = new Array[ImageDataset](horBlocks * verBlocks)

    // 分块并放入数组中
    for (i <- 0 until horBlocks) {
      // 行是控制高度
      val yOff = i * regularImgHeight

      var subBlockHeight = regularImgHeight
      if ((horBlocks - 1) == i) {
        subBlockHeight = leftImgHeight
      }

      for (j <- 0 until verBlocks) {
        // 列是控制宽度
        val xOff = j * regularImgWidth

        var subBlockWidth = regularImgWidth
        if ((verBlocks - 1) == j) {
          subBlockWidth = leftImgWidth
        }

        val block: ImageDataset = inputImage.getSubimage(xOff, yOff, subBlockWidth, subBlockHeight) // 最后两个参数可以为 0
        subImgBlockArr(i * verBlocks + j) = block
      }
    }

    subImgBlockArr
  }

  def temp(): Unit = {
    val inputImagePath = "input.jpg" // 输入图像的路径
    val outputDirectory = "output"   // 输出块图像的目录
    val blockSize = 256             // 块的大小，可以根据需求调整

    // 读取输入图像
    val inputImage = ImageIO.read(new File(inputImagePath))

    // 获取输入图像的宽度和高度
    val imageWidth = inputImage.getWidth
    val imageHeight = inputImage.getHeight

    // 计算水平和垂直块数
    val numHorizontalBlocks = imageWidth / blockSize
    val numVerticalBlocks = imageHeight / blockSize

    // 创建输出目录
    val outputDir = new File(outputDirectory)
    if (!outputDir.exists()) {
      outputDir.mkdirs()
    }

    // 分块并保存
    for (i <- 0 until numHorizontalBlocks) {
      for (j <- 0 until numVerticalBlocks) {
        val x = i * blockSize
        val y = j * blockSize
        val block = inputImage.getSubimage(x, y, blockSize, blockSize)
        val outputFileName = s"block_${i}_${j}.jpg"
        val outputFile = new File(outputDirectory, outputFileName)
        ImageIO.write(block, "jpg", outputFile)
      }
    }

    println(s"分块完成，共生成了 ${numHorizontalBlocks * numVerticalBlocks} 个块图像。")
  }

}










