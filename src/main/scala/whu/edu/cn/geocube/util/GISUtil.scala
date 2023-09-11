package whu.edu.cn.geocube.util

import java.math.BigDecimal

import org.locationtech.jts.geom.Envelope
import org.opengis.geometry.BoundingBox

/**
 * Provide some basic gis operation.
 * */
class GISUtil{

  /**
   * Calculate the salting code of tileDataId.
   * @param tileDataId the id of tile data.
   * @return salting code.
   */
  def salt(tileDataId: Integer): String = {
    val splitsCount = 13
    var rowKey = String.format("%010d", tileDataId)
    val saltingCode = rowKey.hashCode % splitsCount
    var saltingKey = "" + saltingCode
    if (saltingCode < 10) saltingKey = "00" + saltingKey
    else if (saltingCode < 100) saltingKey = "0" + saltingKey
    rowKey = saltingKey + "~" + rowKey
    rowKey
  }


  /**
   * Transform extent into WKT.
   * @param minx minx of extent
   * @param miny maxx of extent
   * @param maxx miny of extent
   * @param maxy maxx of extent
   * @return a WKT String
   */
  def DoubleToWKT(minx: Double, miny: Double, maxx: Double, maxy: Double): String = {
    val WKT_rec = "POLYGON((" + minx + " " + miny + ", " + minx + " " + maxy + ", " + maxx + " " + maxy + ", " + maxx + " " + miny + "," + minx + " " + miny + "))"
    WKT_rec
  }

  /**
   * Calculate ESPG code of a point.
   * @param BottomLeftLongitude Longitude of the point.
   * @param BottomLeftLatitude Latitude of the point.
   * @return ESPG code.
   */
  def getLandsatEPSGCode(BottomLeftLongitude: Double, BottomLeftLatitude: Double): Integer = {
    val lng = (183 + BottomLeftLongitude) / 6
    val Zone = new BigDecimal(lng).setScale(0, BigDecimal.ROUND_HALF_UP).intValue
    val lat = (45 + BottomLeftLatitude) / 90
    val value = new BigDecimal(lat).setScale(0, BigDecimal.ROUND_HALF_UP).intValue
    val EPSG = 32700 - value * 100 + Zone
    EPSG
  }

}
object GISUtil{
  /**
   * Calculate extent of the input BoundingBox array.
   *
   * @param x
   * @return
   */
  def getVectorExtent(x: (String, Iterable[BoundingBox])): (Double, Double, Double, Double) = {
    var minx = Double.MaxValue
    var maxx = Double.MinValue
    var miny = Double.MaxValue
    var maxy = Double.MinValue
    for (bound <- x._2) {
      val boundsMaxX = bound.getMaxX
      val boundsMaxY = bound.getMaxY
      val boundsMinX = bound.getMinX
      val boundsMinY = bound.getMinY
      if (boundsMaxX > maxx) {
        maxx = boundsMaxX
      }
      if (boundsMinX < minx) {
        minx = boundsMinX
      }
      if (boundsMaxY > maxy) {
        maxy = boundsMaxY
      }
      if (boundsMinY < miny) {
        miny = boundsMinY
      }
    }
    (maxx, minx, maxy, miny)
  }

  /**
   * Calculate extent of the input Envelope array.
   *
   * @param x
   * @return
   */
  def getVectorExtent2(x: (String, Iterable[Envelope])): (Double, Double, Double, Double) = {
    var minx = Double.MaxValue
    var maxx = Double.MinValue
    var miny = Double.MaxValue
    var maxy = Double.MinValue
    for (envelope <- x._2) {
      val envelopeMaxX = envelope.getMaxX
      val envelopeMaxY = envelope.getMaxY
      val envelopeMinX = envelope.getMinX
      val envelopeMinY = envelope.getMinY
      if (envelopeMaxX > maxx) {
        maxx = envelopeMaxX
      }
      if (envelopeMinX < minx) {
        minx = envelopeMinX
      }
      if (envelopeMaxY > maxy) {
        maxy = envelopeMaxY
      }
      if (envelopeMinY < miny) {
        miny = envelopeMinY
      }
    }
    (maxx, minx, maxy, miny)
  }
}
