package whu.edu.cn.algorithms.terrain.core

import scala.math.Pi

class TileCellAddressor {

  private var mtype: Int = _
  private var radius: Double = 1.0
  private var radius0: Double = 0.0
  private var direction: Double = 0.0
  private var tolerance: Double = 0.0

  var kernel: TileTable = new TileTable()
  var weighting: TileDistanceWeighting = new TileDistanceWeighting()

  def setRadius(Radius: Double, bSquare: Boolean): Boolean = {
    if (bSquare) {
      setSquare(Radius)
    } else {
      setCircle(Radius)
    }
  }

  def setSquare(Radius: Double): Boolean = {
    setKernel(0, Radius, 0.0, 0.0, 0.0)
  }

  def setCircle(Radius: Double): Boolean = {
    setKernel(1, Radius, 0.0, 0.0, 0.0)
  }

  def setKernel(
      Type: Int,
      Radius: Double,
      RadiusInner: Double,
      Direction: Double,
      Tolerance: Double
  ): Boolean = {
    mtype = Type
    radius = Radius
    radius0 = RadiusInner
    direction = Direction % (2 * Pi)
    tolerance = Tolerance % Pi

    if (radius < 0 || radius < radius0) {
      return false
    }

    val Kernel: TileTable = TileTable.create(kernel)
    val size: Int = math.ceil(radius).toInt

    for (y <- -size to size) {
      if (math.abs(y) <= radius) {
        for (x <- -size to size) {
          if (math.abs(x) <= radius) {
            val d: Double = math.sqrt(x * x + y * y)
            mtype match {
              case 1 => if (d <= radius) addCell(x, y, d, Kernel)
              case 2 =>
                if (d <= radius && d >= radius0) addCell(x, y, d, Kernel)
              case _ => addCell(x, y, d, Kernel)
            }
          }
        }
      }
    }
    if (Kernel.nRecords < 1) return false
    Kernel.setIndex(2, TABLEINDEXAscending)

    for (i <- 0 until Kernel.nRecords) {
      kernel.addRecord(Kernel.getRecordByIndex(i))
    }
    true
  }

  def addCell(x: Int, y: Int, Distance: Double, Kernel: TileTable): Unit = {
    val cell: TileTableRecord = Kernel.addRecord()
    cell.setValue(0, x)
    cell.setValue(1, y)
    cell.setValue(2, Distance)
    cell.setValue(3, weighting.getWeight(Distance))
  }

  def getValues(
      Index: Int,
      x: Int,
      y: Int,
      Distance: Double,
      Weight: Double,
      bOffset: Boolean = false
  ): (Int, Int, Double, Double, Boolean) = {
    var X: Int = x
    var Y: Int = y
    var distance: Double = Distance
    var weight: Double = Weight
    if (Index >= 0 && Index < kernel.nRecords) {
      val Cell: TileTableRecord = kernel(Index)
      if (bOffset) {
        X = x + Cell.asInt(0)
        Y = y + Cell.asInt(1)
      } else {
        X = Cell.asInt(0)
        Y = Cell.asInt(1)
      }
      distance = Cell.asDouble(2)
      weight = Cell.asDouble(3)

      return (X, Y, distance, weight, true)
    }
    (X, Y, distance, weight, false)
  }

}

object TileCellAddressor {
  def create(): TileCellAddressor = {
    val result = new TileCellAddressor()
    result.kernel.addField("X", DATATYPEInt)
    result.kernel.addField("Y", DATATYPEInt)
    result.kernel.addField("D", DATATYPEDouble)
    result.kernel.addField("W", DATATYPEDouble)

    result
  }

}
