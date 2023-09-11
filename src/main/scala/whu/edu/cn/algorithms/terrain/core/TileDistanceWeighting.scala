package whu.edu.cn.algorithms.terrain.core

class TileDistanceWeighting {
  var bandwidth: Double = 1.0
  var IDWPower: Double = 2.0
  var IDWbOffset: Boolean = true
  var weighting: DistanceWeighting = DISTWGHTNone

  def setBandWidth(Value: Double): Boolean = {
    if (Value <= 0) {
      return false
    }
    bandwidth = Value
    true
  }

  def getWeight(Distance: Double): Double = {
    if (Distance < 0.0) {
      return 0.0
    }

    weighting match {
      case DISTWGHTIDW =>
        if (IDWbOffset) {
          math.pow(1.0 + Distance, -IDWPower)
        } else if (Distance > 0) {
          math.pow(Distance, -IDWPower)
        } else {
          0.0
        }
      case DISTWGHTEXP => math.exp(-Distance / bandwidth)
      case DISTWGHTGAUSS =>
        val normalizedDistance = Distance / bandwidth
        math.exp(-0.5 * normalizedDistance * normalizedDistance)
      case _ => 1.0
    }
  }

}
