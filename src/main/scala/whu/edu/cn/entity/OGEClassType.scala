package whu.edu.cn.entity

object OGEClassType extends Enumeration with Serializable {
  type OGEClassType = Value
  val Service, Coverage, CoverageCollection, Feature, FeatureCollection, CoverageArray = Value
}
