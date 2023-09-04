package whu.edu.cn.oge

import geotrellis.vector.Extent
import whu.edu.cn.entity.CoverageCollectionMetadata
import whu.edu.cn.trigger.Trigger

import java.time.{LocalDateTime, ZonedDateTime}
import scala.collection.mutable

object Filter {
  def func(coverageCollectionMetadata: CoverageCollectionMetadata, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {
    funcName match {
      case "Filter.equals" =>
        Filter.equals(coverageCollectionMetadata, leftField = args("leftField"), rightValue = args("rightValue"))
      case "Filter.and" =>
        Filter.and(coverageCollectionMetadata, filters = args("filters"))
      case "Filter.date" =>
        Filter.date(coverageCollectionMetadata, args("startTime"), args("endTime"))
      case "Filter.bounds" =>
        Filter.bounds(coverageCollectionMetadata,args("bounds"))
    }
  }

  def equals(coverageCollectionMetadata: CoverageCollectionMetadata, leftField: String, rightValue: String): CoverageCollectionMetadata = {
    leftField match {
      case "sensorName" =>
        coverageCollectionMetadata.setSensorName(rightValue)
      case "measurementName" =>
        coverageCollectionMetadata.setMeasurementName(rightValue)
      case "crs" =>
        coverageCollectionMetadata.setCrs(rightValue)
    }
    coverageCollectionMetadata
  }

  def and(coverageCollectionMetadata: CoverageCollectionMetadata, filters: String): CoverageCollectionMetadata = {
    val filterList: Array[String] = filters.replace("[", "").replace("]", "").split(",")
    for (filter <- filterList) {
      val tuple: (String, mutable.Map[String, String]) = Trigger.lazyFunc(filter)
      Filter.func(coverageCollectionMetadata, filter, tuple._1, tuple._2)
    }
    coverageCollectionMetadata
  }

  def date(coverageCollectionMetadata: CoverageCollectionMetadata, startTime: String, endTime: String)
  : CoverageCollectionMetadata = {
    val start: LocalDateTime = LocalDateTime.parse(startTime)
    val end: LocalDateTime = LocalDateTime.parse(endTime)
    coverageCollectionMetadata.startTime = start
    coverageCollectionMetadata.endTime = end
    coverageCollectionMetadata
  }

  // bounds: A string of the form "xmin,ymin,xmax,ymax"
  def bounds(coverageCollectionMetadata: CoverageCollectionMetadata, bounds: String): CoverageCollectionMetadata = {
    coverageCollectionMetadata.extent = Extent.fromString(bounds)
    coverageCollectionMetadata
  }
}
