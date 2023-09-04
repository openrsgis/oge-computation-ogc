package whu.edu.cn.oge

import geotrellis.layer.stitch.TileLayoutStitcher
import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.mapalgebra.focal
import geotrellis.raster.mapalgebra.focal.{Aspect, Slope}
import geotrellis.raster.{ByteArrayTile, ByteCellType, CellType, MultibandTile, TargetCell, Tile}
import org.apache.spark.rdd.RDD
import whu.edu.cn.entity
import whu.edu.cn.entity.SpaceTimeBandKey

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object Terrain {

//  def slope(coverage: (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//            radius: Int, zFactor: Double): (RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]) = {
//    val neighbor = focal.Square(radius)
//    val cellSize = coverage._2.cellSize
//    val leftNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(0, 1), t._2))
//    })
//    val rightNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(2, 1), t._2))
//    })
//    val upNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(1, 0), t._2))
//    })
//    val downNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(1, 2), t._2))
//    })
//    val leftUpNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(0, 0), t._2))
//    })
//    val upRightNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(2, 0), t._2))
//    })
//    val rightDownNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(2, 2), t._2))
//    })
//    val downLeftNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(0, 2), t._2))
//    })
//    val midNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(1, 1), t._2))
//    })
//    //合并邻域RDD
//    val unionRDD = leftNeighborRDD.union(rightNeighborRDD).union(upNeighborRDD).union(downNeighborRDD).union(leftUpNeighborRDD).union(upRightNeighborRDD).
//      union(rightDownNeighborRDD).union(downLeftNeighborRDD).union(midNeighborRDD)
//      .filter(t => {
//        t._1.spaceTimeKey.spatialKey._1 >= 0 && t._1.spaceTimeKey.spatialKey._2 >= 0 && t._1.spaceTimeKey.spatialKey._1 < coverage._2.layout.layoutCols &&
//          t._1.spaceTimeKey.spatialKey._2 < coverage._2.layout.layoutRows
//      })
//
//    val groupRDD = unionRDD.groupByKey().map(t => {
//      //处理边缘部分只包含4、6瓦片的情况，增加无数据瓦片至9瓦片，
//      val listBuffer = new ListBuffer[(SpatialKey, Tile)]()
//      val list = t._2.toList
//      for (key <- List(SpatialKey(0, 0), SpatialKey(0, 1), SpatialKey(0, 2), SpatialKey(1, 0), SpatialKey(1, 1), SpatialKey(1, 2), SpatialKey(2, 0), SpatialKey(2, 1), SpatialKey(2, 2))) {
//        var flag = false
//        breakable {
//          for (tile <- list) {
//            if (key.equals(tile._1)) {
//              listBuffer.append(tile)
//              flag = true
//              break
//            }
//          }
//        }
//        if (flag == false) {
//          listBuffer.append((key, ByteArrayTile(Array.fill[Byte](256 * 256)(-128), 256, 256, ByteCellType)))
//        }
//      }
//      //拼接瓦片并切割，使瓦片较原先的256×256增加了5像素宽度的边缘重复区域
//      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(listBuffer)
//      (t._1, tile.crop(251, 251, 516, 516).convert(CellType.fromName("int16")))
//    })
//    val slopeRDD = groupRDD.map(t => {
//      (t._1, Slope(t._2, neighbor, None, cellSize, zFactor, TargetCell.All).crop(5,5,260,260))
//    })
//    (slopeRDD, TileLayerMetadata(CellType.fromName("int16"),coverage._2.layout,coverage._2.extent,coverage._2.crs,coverage._2.bounds))
//  }
//
//
//  def aspect(coverage:(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey]),
//             radius:Int):(RDD[(SpaceTimeBandKey, MultibandTile)], TileLayerMetadata[SpaceTimeKey])={
//    val neighbor = focal.Square(radius)
//    val cellSize = coverage._2.cellSize
//    val leftNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(0, 1), t._2))
//    })
//    val rightNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(2, 1), t._2))
//    })
//    val upNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(1, 0), t._2))
//    })
//    val downNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(1, 2), t._2))
//    })
//    val leftUpNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(0, 0), t._2))
//    })
//    val upRightNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row + 1, 0), t._1.measurementName), (SpatialKey(2, 0), t._2))
//    })
//    val rightDownNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col - 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(2, 2), t._2))
//    })
//    val downLeftNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col + 1, t._1.spaceTimeKey.row - 1, 0), t._1.measurementName), (SpatialKey(0, 2), t._2))
//    })
//    val midNeighborRDD = coverage._1.map(t => {
//      (entity.SpaceTimeBandKey(SpaceTimeKey(t._1.spaceTimeKey.col, t._1.spaceTimeKey.row, 0), t._1.measurementName), (SpatialKey(1, 1), t._2))
//    })
//    //合并邻域RDD
//    val unionRDD = leftNeighborRDD.union(rightNeighborRDD).union(upNeighborRDD).union(downNeighborRDD).union(leftUpNeighborRDD).union(upRightNeighborRDD).
//      union(rightDownNeighborRDD).union(downLeftNeighborRDD).union(midNeighborRDD)
//      .filter(t => {
//        t._1.spaceTimeKey.spatialKey._1 >= 0 && t._1.spaceTimeKey.spatialKey._2 >= 0 && t._1.spaceTimeKey.spatialKey._1 < coverage._2.layout.layoutCols &&
//          t._1.spaceTimeKey.spatialKey._2 < coverage._2.layout.layoutRows
//      })
//
//    val groupRDD = unionRDD.groupByKey().map(t => {
//      //处理边缘部分只包含4、6瓦片的情况，增加无数据瓦片至9瓦片，
//      val listBuffer = new ListBuffer[(SpatialKey, Tile)]()
//      val list = t._2.toList
//      for (key <- List(SpatialKey(0, 0), SpatialKey(0, 1), SpatialKey(0, 2), SpatialKey(1, 0), SpatialKey(1, 1), SpatialKey(1, 2), SpatialKey(2, 0), SpatialKey(2, 1), SpatialKey(2, 2))) {
//        var flag = false
//        breakable {
//          for (tile <- list) {
//            if (key.equals(tile._1)) {
//              listBuffer.append(tile)
//              flag = true
//              break
//            }
//          }
//        }
//        if (flag == false) {
//          listBuffer.append((key, ByteArrayTile(Array.fill[Byte](256 * 256)(-128), 256, 256, ByteCellType)))
//        }
//      }
//      //拼接瓦片并切割，使瓦片较原先的256×256增加了5像素宽度的边缘重复区域
//      val (tile, (_, _), (_, _)) = TileLayoutStitcher.stitch(listBuffer)
//      (t._1, tile.crop(251, 251, 516, 516).convert(CellType.fromName("int16")))
//    })
//
//    val aspectRDD = groupRDD.map(t => {
//      (t._1, Aspect(t._2, neighbor, None, cellSize, TargetCell.All).crop(5,5,260,260))
//    })
//    (aspectRDD, TileLayerMetadata(CellType.fromName("int16"),coverage._2.layout,coverage._2.extent,coverage._2.crs,coverage._2.bounds))
//  }
}
