package whu.edu.cn.algorithms.terrain.core

import geotrellis.layer.{SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{ByteConstantNoDataCellType, ByteUserDefinedNoDataCellType, DoubleArrayTile, DoubleCellType, DoubleCells, DoubleConstantNoDataCellType, DoubleUserDefinedNoDataCellType, FloatConstantNoDataCellType, FloatUserDefinedNoDataCellType, IntConstantNoDataCellType, IntUserDefinedNoDataCellType, NoDataHandling, ShortConstantNoDataCellType, ShortUserDefinedNoDataCellType, Tile, UByteConstantNoDataCellType, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType, UShortUserDefinedNoDataCellType}

import scala.math.{Pi, atan, atan2, sqrt}
import scala.util.control.Breaks.{break, breakable}

/**
 * a wrapper class for [[geotrellis.raster.DoubleArrayTile]]
 * @param arr         array for current splice
 * @param cols        num of columns
 * @param rows        num of rows
 * @param metaData    meta data of the raster
 * @param paddingSize paddingSize for current splice
 */
final case class OgeTerrainTile(
                                 arr: Array[Double], cols: Int, rows: Int,
                                 metaData: TileLayerMetadata[SpaceTimeKey],
                                 paddingSize: Int = 0
                               ) extends DoubleArrayTile(arr, cols, rows) {

  override val cellType: DoubleCells with NoDataHandling = DoubleCellType

  override def apply(i: Int): Int = arr(i).round.toInt

  override def applyDouble(i: Int): Double = arr(i)

  override def update(i: Int, z: Int): Unit = { arr(i) = z.toDouble }

  override def updateDouble(i: Int, z: Double): Unit = { arr(i) = z }

  override def get(col: Int, row: Int): Int = apply((rows - row - 1) * cols + col)

  override def getDouble(col: Int, row: Int): Double = applyDouble((rows - row - 1) * cols + col)

  def getDouble(i: Int): Double = getDouble(i % nX, i / nX)

  override def set(col: Int, row: Int, value: Int): Unit = {
    update((rows - row - 1) * cols + col, value)
  }

  override def setDouble(col: Int, row: Int, value: Double): Unit = {
    updateDouble((rows - row - 1) * cols + col, value)
  }

  def setDouble(i: Int, value: Double): Unit = setDouble(i % nX, i / nX, value)

  /** do some post-processing tasks include crop the raster and redefine noDataValue
   *
   * @return raster data of type [[Tile]] after the refine process
   */
  def refine(): Tile = {
    this.crop(paddingSize, paddingSize, nX - paddingSize - 1, nY - paddingSize - 1).withNoData(Option(noDataValue))
  }

  /** this function is used to calculate `X` index at the direction of `dir` from `x`
   *
   * @param dir direction
   * @param x   origin x position
   * @return `X` position at the dircection of `dir` from `x`
   * @note direction is described as follow:
   *       <table cellspacing=8">
   *       <tr>
   *       <th>7</th>
   *       <th>0</th>
   *       <th>1</th>
   *       </tr>
   *       <tr>
   *       <th>6</th>
   *       <th>*</th>
   *       <th>2</th>
   *       </tr>
   *       <tr>
   *       <th>5</th>
   *       <th>4</th>
   *       <th>3</th>
   *       </tr>
   *       </table>
   */
  def calXTo(dir: Int, x: Int = 0): Int = {
    val ix: Array[Int] = Array(0, 1, 1, 1, 0, -1, -1, -1)
    var direction = dir % 8
    if (direction < 0) {
      direction += 8
    }
    x + ix(direction)
  }


  /** this function is used to calculate `Y` index at the direction of `dir` from `y`
   *
   * @param dir direction
   * @param y   origin y position
   * @return `Y` position at the dircection of `dir` from `y`
   * @note direction is described as follow:
   *       <table cellspacing=8">
   *       <tr>
   *       <th>7</th>
   *       <th>0</th>
   *       <th>1</th>
   *       </tr>
   *       <tr>
   *       <th>6</th>
   *       <th>*</th>
   *       <th>2</th>
   *       </tr>
   *       <tr>
   *       <th>5</th>
   *       <th>4</th>
   *       <th>3</th>
   *       </tr>
   *       </table>
   */
  def calYTo(dir: Int, y: Int = 0): Int = {
    val iy: Array[Int] = Array(1, 1, 0, -1, -1, -1, 0, 1)
    var direction = dir % 8
    if (direction < 0) {
      direction += 8
    }
    y + iy(direction)
  }


  /** this function is used to calculat `X` index at the reversed direction of `dir` from `x`
   *
   * @param dir direction
   * @param x   origin x position
   * @return `X` position at the reversed direction of `dir` from `x`
   * @note the reversed direction is described as follow:
   *       <table cellspacing=8>
   *       <tr>
   *       <th>3</th>
   *       <th>4</th>
   *       <th>5</th>
   *       </tr>
   *       <tr>
   *       <th>2</th>
   *       <th>*</th>
   *       <th>6</th>
   *       </tr>
   *       <tr>
   *       <th>1</th>
   *       <th>0</th>
   *       <th>7</th>
   *       </tr>
   *       </table>
   */
  def calXFrom(dir: Int, x: Int): Int = calXTo(dir + 4, x)


  /** this function is used to calculat `Y` index at the reversed direction of `dir` from `y`
   *
   * @param dir direction
   * @param y   origin y position
   * @return `Y` position at the reversed direction of `dir` from `y`
   * @note the reversed direction is described as follow:
   *       <table cellspacing=8>
   *       <tr>
   *       <th>3</th>
   *       <th>4</th>
   *       <th>5</th>
   *       </tr>
   *       <tr>
   *       <th>2</th>
   *       <th>*</th>
   *       <th>6</th>
   *       </tr>
   *       <tr>
   *       <th>1</th>
   *       <th>0</th>
   *       <th>7</th>
   *       </tr>
   *       </table>
   */
  def calYFrom(dir: Int, y: Int): Int = calYTo(dir + 4, y)

  /** check if Point at position `(x, y)` is valid
   *
   * @param x x position
   * @param y y position
   * @return point is vaild or not
   */
  def isInGrid(x: Int, y: Int): Boolean = x >= 0 && x < nX && y >= 0 && y < nY && !isNoData(x, y)

  /** check if Point at position `(i)` is valid
   *
   * @param i raw position
   * @return point is vaild or not
   */
  def isInGrid(i: Int): Boolean = isInGrid(i % nX, i / nX)

  /** check if Point at position `(x, y)` is nodata or not
   *
   * @param x x position
   * @param y y position
   * @return point is nodata or not
   */
  def isNoData(x: Int, y: Int): Boolean = getDouble(x, y).isNaN || getDouble(x, y) == noDataValue

  /** check if Point at position `(i)` is nodata or not
   *
   * @param i raw position
   * @return point is nodata or not
   */
  def isNoData(i: Int): Boolean = getDouble(i).isNaN || getDouble(i) == noDataValue

  def setNoData(x: Int, y: Int): Unit = setDouble(x, y, noDataValue)

  def setNoData(i: Int): Unit = setDouble(i, noDataValue)

  def setIndex(): Boolean = {
    if (index == null) index = new Array[Int](nCells)

    val M = 7
    var i, j, k, l, ir, n, jstack, nstack, indxt, itemp, nData: Int = 0
    var a: Double = 0
    nData = nCells

    i = 0
    j = 0
    while (i < nCells) {
      if (isNoData(i)) {
        nData -= 1
        index(nData) = i
      } else {
        index(j) = i
        j += 1
      }
      i += 1
    }

    if (nData <= 0) return false

    l = 0
    n = 0
    ir = nData - 1

    nstack = 64
    var istack = new Array[Int](nstack)
    jstack = 0

    breakable {
      while (true) {
        if (ir - l < M) {
          j = l + 1
          while (j <= ir) {
            indxt = index(j)
            a = getDouble(indxt)
            breakable {
              i = j - 1
              while (i >= 0) {
                if (getDouble(index(i)) <= a) {
                  break
                }
                index(i + 1) = index(i)
                i -= 1
              }
            }
            index(i + 1) = indxt
            j += 1
          }
          if (jstack == 0) break
          ir = istack(jstack)
          jstack -= 1
          l = istack(jstack)
          jstack -= 1
        } else {
          k = (l + ir) / 2
          swapIndex(k, l + 1)
          if (getDouble(index(l + 1)) > getDouble(index(ir))) {
            swapIndex(l + 1, ir)
          }
          if (getDouble(index(l)) > getDouble(index(ir))) {
            swapIndex(l, ir)
          }
          if (getDouble(index(l + 1)) > getDouble(index(l))) {
            swapIndex(l + 1, l)
          }
          i = l + 1
          j = ir
          indxt = index(l)
          a = getDouble(indxt)

          breakable {
            while (true) {
              do i += 1 while (getDouble(index(i)) < a)
              do j -= 1 while (getDouble(index(j)) > a)
              if (j < i) break
              swapIndex(i, j)
            }
          }

          index(l) = index(j)
          index(j) = indxt
          jstack += 2
          if (jstack >= nstack) {
            nstack += 64
            istack ++= Array.ofDim[Int](nstack - istack.length)
          }

          if (ir - i + 1 >= j - l) {
            istack(jstack) = ir
            istack(jstack - 1) = i
            ir = j - 1
          } else {
            istack(jstack) = j - 1
            istack(jstack - 1) = l
            l = i
          }
        }
      }
    }
    true
  }

  def swapIndex(i: Int, j: Int): Unit = {
    val tmp = index(i)
    index(i) = index(j)
    index(j) = tmp
  }

  /** this function is used to calculate the `gradient` at the position `x`,`y`
   *
   * @param x x of position
   * @param y y of position
   * @return `slope` the slope of position <BR/>
   *         `aspect` the aspect of position
   *         <P/>
   *         `bool` if calculte successfully,return `true`,if position is not in grid ,return `false`
   */
  def getGradient(x: Int, y: Int): (Double, Double, Boolean) = {
    var Slope, Aspect: Double = 0.0
    if (isInGrid(x, y)) {
      val z: Double = getDouble(x, y)
      val dz: Array[Double] = new Array[Double](4)

      var iDir: Int = 0
      for (i <- 0 until 4) {
        var ix, iy: Int = 0

        ix = calXTo(iDir, x)
        iy = calYTo(iDir, y)

        if (isInGrid(ix, iy)) {
          dz(i) = getDouble(ix, iy) - z
        } else {
          ix = calXFrom(iDir, x)
          iy = calYFrom(iDir, y)
          if (isInGrid(ix, iy)) {
            dz(i) = z - getDouble(ix, iy)
          } else {
            dz(i) = 0
          }
        }

        iDir += 2
      }

      val G: Double = (dz(0) - dz(2)) / (2.0 * cellSize)
      val H: Double = (dz(1) - dz(3)) / (2.0 * cellSize)

      Slope = atan(sqrt(G * G + H * H))
      if (G != 0) {
        Aspect = Pi + atan2(H, G)
      } else if (H > 0) {
        Aspect = Pi * 3.0 / 2.0
      } else if (H < 0) {
        Aspect = Pi / 2.0
      } else {
        Aspect = -1.0
      }

      (Slope, Aspect, true)
    } else {
      Slope = 0.0
      Aspect = -1.0
      (Slope, Aspect, false)
    }
  }

  def getSorted(Position: Int, bDown: Boolean = true, bCheckNoData: Boolean = true): (Int, Int, Boolean) = {
    var position: Int = 0
    if (Position >= 0 && Position < nCells) {
      if (bDown) {
        position = index(nCells - Position - 1)
      } else {
        position = index(Position)
      }

      if (!bCheckNoData || !isNoData(position)) {
        position = position
      } else {
        position = -1
      }
    } else {
      position = -1
    }

    val x: Int = position % nX
    val y: Int = position / nX

    if (position >= 0) {
      (x, y, true)
    } else {
      (x, y, false)
    }
  }

  def getGradientNeighborDir(x: Int, y: Int, bDown: Boolean = true, bNoEdges: Boolean = true): Int = {
    var Direction: Int = -1
    if (isInGrid(x, y)) {
      val z: Double = getDouble(x, y)
      var dzMax: Double = 0.0

      for (i <- 0 until 8) {
        val ix: Int = calXTo(i, x)
        val iy: Int = calYTo(i, y)

        if (isInGrid(ix, iy)) {
          val dz: Double = (z - getDouble(ix, iy)) / getLength(i)
          if ((!bDown || dz > 0) && (Direction < 0 || dzMax < dz)) {
            dzMax = dz
            Direction = i
          }
        } else if (bNoEdges) {
          return -1
        }
      }
    }
    Direction
  }

  def getLength(dir: Int): Double = if (dir % 2 != 0) diagonal else cellSize

  def getUnitLength(dir: Int): Double = if(dir % 2 != 0) math.sqrt(2.0) else 1.0

  /**
   * cellSize of the raster, we expect x y directions of the raster to have consistent resolutions.
   */
  val cellSize: Double = if(metaData.cellSize.width < 1) metaData.cellSize.width * 111192.41337043587 else metaData.cellSize.width
  //  val cellSize: Double = metaData.cellSize.width

  /**
   * area of each cell
   */
  val cellArea: Double = cellSize * cellSize

  /**
   * length of diagnal of each cell
   */
  val diagonal: Double = cellSize * sqrt(2)

  /**
   * same as cols
   */
  val nX: Int = cols

  /**
   * same as rows
   */
  val nY: Int = rows

  /**
   * num of cells
   */
  val nCells: Int = nX * nY

  /**
   * noDataValue of the raster
   */
  var noDataValue: Double = metaData.cellType match {
    case x: DoubleUserDefinedNoDataCellType => x.noDataValue
    case x: FloatUserDefinedNoDataCellType => x.noDataValue.toDouble
    case x: IntUserDefinedNoDataCellType => x.noDataValue.toDouble
    case x: ShortUserDefinedNoDataCellType => x.noDataValue.toDouble
    case x: UShortUserDefinedNoDataCellType => x.noDataValue.toDouble
    case x: ByteUserDefinedNoDataCellType => x.noDataValue.toDouble
    case x: UByteUserDefinedNoDataCellType => x.noDataValue.toDouble
    case ByteConstantNoDataCellType => ByteConstantNoDataCellType.noDataValue.toDouble
    case UByteConstantNoDataCellType => UByteConstantNoDataCellType.noDataValue.toDouble
    case ShortConstantNoDataCellType => ShortConstantNoDataCellType.noDataValue.toDouble
    case UShortConstantNoDataCellType => UShortConstantNoDataCellType.noDataValue.toDouble
    case IntConstantNoDataCellType => IntConstantNoDataCellType.noDataValue.toDouble
    case FloatConstantNoDataCellType => FloatConstantNoDataCellType.noDataValue.toDouble
    case DoubleConstantNoDataCellType => DoubleConstantNoDataCellType.noDataValue
    case _ => Double.NaN
  }

  /**
   * index array used for computational assistance
   */
  var index: Array[Int] = _

}

object OgeTerrainTile {

  /**
   * create an [[OgeTerrainTile]] use specified data
   * @param arr         array for for current splice
   * @param cols        num of columns
   * @param rows        num of rows
   * @param metaData    metaData for the raster
   * @param paddingSize paddingSize for current splice
   * @return [[OgeTerrainTile]]
   */
  def from(arr: Array[Double], cols: Int, rows: Int,
           metaData: TileLayerMetadata[SpaceTimeKey], paddingSize: Int
          ): OgeTerrainTile = {
    new OgeTerrainTile(arr, cols, rows, metaData, paddingSize)
  }

  /**
   * create an [[OgeTerrainTile]] use specified data
   *
   * @param tile        data for current splice with format [[Tile]]
   * @param metaData    metaData for the raster
   * @param paddingSize paddingSize for current splice
   * @return [[OgeTerrainTile]]
   */
  def from(tile: Tile, metaData: TileLayerMetadata[SpaceTimeKey], paddingSize: Int): OgeTerrainTile = {
    new OgeTerrainTile(tile.toArrayDouble(), tile.cols, tile.rows, metaData, paddingSize)
  }

  /**
   * create an [[OgeTerrainTile]] with the same shape and metaData as `tile`
   *
   * @param tile        result will as the same shape of `tile`
   * @param metaData    metaData for the raster
   * @param paddingSize paddingSize for current splice
   * @return [[OgeTerrainTile]]
   */
  def like(tile: Tile, metaData: TileLayerMetadata[SpaceTimeKey], paddingSize: Int): OgeTerrainTile = {
    val noDataValue: Double = metaData.cellType match {
      case x: DoubleUserDefinedNoDataCellType => x.noDataValue
      case x: FloatUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: IntUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: ShortUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: UShortUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: ByteUserDefinedNoDataCellType => x.noDataValue.toDouble
      case x: UByteUserDefinedNoDataCellType => x.noDataValue.toDouble
      case ByteConstantNoDataCellType => ByteConstantNoDataCellType.noDataValue.toDouble
      case UByteConstantNoDataCellType => UByteConstantNoDataCellType.noDataValue.toDouble
      case ShortConstantNoDataCellType => ShortConstantNoDataCellType.noDataValue.toDouble
      case UShortConstantNoDataCellType => UShortConstantNoDataCellType.noDataValue.toDouble
      case IntConstantNoDataCellType => IntConstantNoDataCellType.noDataValue.toDouble
      case FloatConstantNoDataCellType => FloatConstantNoDataCellType.noDataValue.toDouble
      case DoubleConstantNoDataCellType => DoubleConstantNoDataCellType.noDataValue
      case _ => Double.NaN
    }
    new OgeTerrainTile(
      Array.fill[Double](tile.cols * tile.rows)(noDataValue),
      tile.cols, tile.rows, metaData, paddingSize
    )
  }

  def fill(tile: Tile, metadata: TileLayerMetadata[SpaceTimeKey], paddingSize: Int,value: Double): OgeTerrainTile = {
    new OgeTerrainTile(
      Array.fill[Double](tile.cols * tile.rows)(value),
      tile.cols, tile.rows, metadata, paddingSize)
  }

}
