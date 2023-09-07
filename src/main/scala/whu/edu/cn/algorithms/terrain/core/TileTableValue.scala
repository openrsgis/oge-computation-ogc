package whu.edu.cn.algorithms.terrain.core

abstract class TileTableValue {
  def setValue(Value: Int): Boolean
  def setValue(Value: Double): Boolean
  def asInt(): Int
  def asDouble(): Double
}

class TileTableValueInt extends TileTableValue {

  var value: Int = 0

  override def setValue(Value: Int): Boolean = {
    if (value != Value) {
      value = Value
      return true
    }
    false
  }

  override def setValue(Value: Double): Boolean = setValue(Value.toInt)

  override def asInt(): Int = value

  override def asDouble(): Double = value

}

class TileTableValueDouble extends TileTableValue {

  var value: Double = 0.0

  override def setValue(Value: Int): Boolean = setValue(Value.toDouble)

  override def setValue(Value: Double): Boolean = {
    if (value != Value) {
      value = Value
      return true
    }
    false
  }

  override def asInt(): Int = value.toInt

  override def asDouble(): Double = value
}
