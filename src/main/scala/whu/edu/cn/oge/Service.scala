package whu.edu.cn.oge

import whu.edu.cn.entity.CoverageCollectionMetadata

object Service {

  def getCoverageCollection(productName: String, dateTime: String = null, extent: String = null): CoverageCollectionMetadata = {
    val coverageCollectionMetadata: CoverageCollectionMetadata = new CoverageCollectionMetadata()
    coverageCollectionMetadata.setProductName(productName)
    if (extent.nonEmpty) {
      coverageCollectionMetadata.setExtent(extent)
    }
    if (dateTime.nonEmpty) {
      val timeArray = dateTime.replace("[", "").replace("]", "").split(",")
      coverageCollectionMetadata.setStartTime(timeArray.head)
      coverageCollectionMetadata.setEndTime(timeArray(1))
    }
    coverageCollectionMetadata
  }

}
