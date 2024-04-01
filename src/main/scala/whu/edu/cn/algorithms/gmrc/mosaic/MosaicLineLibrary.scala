package whu.edu.cn.algorithms.gmrc.mosaic

import com.sun.jna.{Library, Native}

object MosaicLineLibrary {
  val MOSAIC_LINE: MosaicLineLibrary = Native.load("./lib/dll/mosaicline/libMosaicLine.so", classOf[MosaicLineLibrary])
}

trait MosaicLineLibrary extends Library {
  def GenerateMosaicLine(files: Array[String], outputDir: String): Boolean
}