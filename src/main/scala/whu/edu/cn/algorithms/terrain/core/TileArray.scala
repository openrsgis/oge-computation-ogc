package whu.edu.cn.algorithms.terrain.core

import scala.reflect.ClassTag

class TileArray[T: ClassTag] {

  var nValues: Int = _
  var nBuffer: Int = _
  var values: Array[T] = new Array[T](0)
  var growth: ArrayGrowth = ARRAYGROWTH0

  def incArray(nvalues: Int = 1): Boolean = setArray(nValues + nvalues)

  def setArray(nvalues: Int, bShrink: Boolean = true): Boolean = {
    if (nvalues >= nValues && nvalues <= nBuffer) {
      nValues = nvalues
      return true
    }
    if (nvalues < nValues && !bShrink) {
      nValues = nvalues
      return true
    }
    if (nvalues == 0) {
      nBuffer = 0
      nValues = 0
      values = new Array[T](0)
      return true
    }
    var nbuffer: Int = 0

    growth match {
      case ARRAYGROWTH0 => nbuffer = nvalues
      case ARRAYGROWTH1 =>
        if (nvalues < 100) {
          nbuffer = nvalues
        } else if (nvalues < 1000) {
          nbuffer = (1 + nvalues / 10) * 10
        } else if (nvalues < 10000) {
          nbuffer = (1 + nvalues / 100) * 100
        } else if (nvalues < 100000) {
          nbuffer = (1 + nvalues / 1000) * 1000
        } else {
          nbuffer = (1 + nvalues / 10000) * 10000
        }
      case ARRAYGROWTH2 =>
        if (nvalues < 10) {
          nbuffer = nvalues
        } else if (nvalues < 100) {
          nbuffer = (1 + nvalues / 10) * 10
        } else if (nvalues < 1000) {
          nbuffer = (1 + nvalues / 100) * 100
        } else if (nvalues < 10000) {
          nbuffer = (1 + nvalues / 1000) * 1000
        } else {
          nbuffer = (1 + nvalues / 10000) * 10000
        }
      case ARRAYGROWTH3 =>
        if (nvalues < 1000) {
          nbuffer = (1 + nvalues / 1000) * 1000
        } else if (nvalues < 10000) {
          nbuffer = (1 + nvalues / 10000) * 10000
        } else if (nvalues < 100000) {
          nbuffer = (1 + nvalues / 100000) * 100000
        } else {
          nbuffer = (1 + nvalues / 1000000) * 1000000
        }
    }

    if (nbuffer == nBuffer) {
      nValues = nvalues
      return true
    }

    val Values: Array[T] = values ++ Array.ofDim[T](nbuffer)
    if (Values != null) {
      nBuffer = nbuffer
      nValues = nvalues
      values = Values
      return true
    }
    false
  }

  def getSize: Int = nValues

  def getArray: Array[T] = values

  def getArray(nvalues: Int): Array[T] = {
    setArray(nvalues)
    values
  }

  def getEntry(index: Int): T = values(index)

  def setEntry(index: Int, value: T): Unit = {
    values(index) = value
  }

  def create(
      nvalues: Int = 0,
      Growth: ArrayGrowth = ARRAYGROWTH0
  ): Array[T] = {
    nBuffer = 0
    nValues = 0
    growth = Growth
    getArray(nvalues)
  }
}
