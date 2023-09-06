package whu.edu.cn.algorithms.terrain.core

class TileArrayInt {

  var array: TileArray[Int] = new TileArray()

  def +=(Value: Int): TileArrayInt = {
    add(Value)
    this
  }

  def add(Value: Int): Boolean = {
    if (incArray()) {
      getArray(getSize - 1) = Value
      return true
    }
    false
  }

  def incArray(nValues: Int = 1): Boolean = array.incArray(nValues)

  def getArray: Array[Int] = array.getArray

  def setArray(nValues: Int, bShrink: Boolean = true): Boolean = array.setArray(nValues, bShrink)

  def getSize: Int = array.getSize

  def get(index: Int): Int = array.getEntry(index)

  def set(index: Int, value: Int): Unit = array.setEntry(index, value)

  def apply(index: Int): Int = get(index)

  def update(index: Int, value: Int): Unit = array.setEntry(index, value)

}

object TileArrayInt {
  def create(nValues: Int = 0, Growth: ArrayGrowth = ARRAYGROWTH0): TileArrayInt = {
    val result: TileArrayInt = new TileArrayInt()
    result.array.create(nValues, Growth)
    result
  }
}
