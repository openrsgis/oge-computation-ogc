package whu.edu.cn.oge

import whu.edu.cn.entity.CoverageCollectionMetadata
import whu.edu.cn.trigger.Trigger

import scala.collection.mutable

object Filter {
  def func(coverageCollectionMetadata: CoverageCollectionMetadata, UUID: String, funcName: String, args: mutable.Map[String, String]): Unit = {
    funcName match {
      case "Filter.equals" =>
        Filter.equals(coverageCollectionMetadata, leftField = args("leftField"), rightValue = args("rightValue"))
      case "Filter.and"=>
        Filter.and(coverageCollectionMetadata, filters = args("filters"))
    }
  }

  def equals(coverageCollectionMetadata: CoverageCollectionMetadata, leftField: String, rightValue: String): CoverageCollectionMetadata = {
    leftField match {
      case "sensorName" =>
        coverageCollectionMetadata.setSensorName(rightValue)
      case "measurementName" =>
        coverageCollectionMetadata.setMeasurementName(rightValue)
      case "crs"=>
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
}
