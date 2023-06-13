package whu.edu.cn.geocube.util

import scala.xml.Elem

/**
 * This class is used to generate TMS config file.
 */
class TileMap (val titleName:String, val tileSetList:Elem, val src:String,
               val maxX:String, val maxY:String,
               val minX:String, val minY:String) {
  var title:String =titleName
  var tileSets:Elem=tileSetList

  def toXML={
    <tilemap tilemapservice="" version="1.0.0">
      <title>{titleName}</title>
      <abstract></abstract>
      <srs>{src}</srs>
      <vsrs></vsrs>
      <boundingbox maxx={maxX} maxy={maxY} minx={minX} miny={minY} />
      <origin x={minX} y={minY} />
      <tileformat extension="png" height="256" mime-type="image/png" width="256" />
      {tileSetList}
    </tilemap>
  }
}
