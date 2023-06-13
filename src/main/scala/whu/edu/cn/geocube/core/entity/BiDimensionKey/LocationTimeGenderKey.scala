package whu.edu.cn.geocube.core.entity.BiDimensionKey

import scala.beans.BeanProperty

class LocationTimeGenderKey (){
  var locationName = ""
  var timeValue = ""
  var genderValue = ""
  var locationKey = -1
  var timeKey = -1
  var genderKey = -1

  def this(_locationName: String, _timeValue: String, _genderValue: String){
    this()
    this.locationName = _locationName
    this.timeValue = _timeValue
    this.genderValue = _genderValue
  }

  def this(_locationKey: Int, _timeKey: Int, _genderKey: Int){
    this()
    this.locationKey = _locationKey
    this.timeKey = _timeKey
    this.genderKey = _genderKey
  }
}
