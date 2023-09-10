package whu.edu.cn.geocube.core.entity

import java.sql.{DriverManager, ResultSet}

import scala.beans.BeanProperty

/**
 * Dimension class - quality.
 * Under developing.
 */
case class GcTileQuality(){
  @BeanProperty
  var tileQualityKey: String = ""
  @BeanProperty
  var cloud: String = ""
  @BeanProperty
  var cloudShadow: String = ""

  def this(_tileQualityKey:String = "", _cloud:String = "", _cloudShadow:String = ""){
    this()
    tileQualityKey = _tileQualityKey
    cloud = _cloud
    cloudShadow = _cloudShadow
  }
}

object GcTileQuality{
  /**
   * Get tileQuality info using tileQualityKey, and return a GcTileQuality object.
   *

   * @param tileQualityKey
   * @param connAddr
   * @param user
   * @param password
   *
   * @return a GcTileQuality object
   */
  def getTileQualityByKey(tileQualityKey:String, connAddr: String, user: String, password: String):GcTileQuality={
    val conn = DriverManager.getConnection(connAddr, user, password)
    if(conn!=null){
      try {
        val statement = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        val sql = "select tile_quality_key,cloud,cloudshadow "+
          "from gc_tile_quality where tile_quality_key=" + tileQualityKey + ";"
        val rs = statement.executeQuery(sql)
        val rsArray = new Array[String](3)
        val columnCount = rs.getMetaData().getColumnCount()

        while (rs.next) {
          for (i <- 1 to columnCount)
            rsArray(i-1)=rs.getString(i)
        }
        val quality = new GcTileQuality(rsArray(0),rsArray(1),rsArray(2))
        quality
      } finally {
        conn.close
      }
    } else
      throw new RuntimeException("Null connection!")
  }
}

