package whu.edu.cn.algorithms.terrain.core

class TileTableRecord {
  var flags: Char = _
  var index: Int = _
  var values: Array[TileTableValue] = new Array[TileTableValue](0)
  var table: TileTable = new TileTable()

  def createValue(Type: DataType): TileTableValue = Type match {
    case DATATYPEInt | DATATYPEColor | DATATYPEBit | DATATYPEChar |
        DATATYPEShort | DATATYPEDword | DATATYPEWord | DATATYPELong |
        DATATYPEULong =>
      new TileTableValueInt()
    case DATATYPEDouble | DATATYPEFloat => new TileTableValueDouble()
    case _                              => throw new IllegalArgumentException("Invalid data type")
  }

  def assign(pRecord: TileTableRecord): Boolean = {
    if (pRecord != null) {
      var nField: Int = 0
      if (table.nFields < pRecord.table.nFields) {
        nField = table.nFields
      } else {
        nField = pRecord.table.nFields
      }
      for (iField <- 0 until nField) {
        values(iField) = pRecord.values(iField)
      }
      return true
    }
    false
  }

  def setValue(iField: Int, Value: Double): Boolean = {
    if (iField >= 0 && iField < table.nFields) {
      if (values(iField).setValue(Value)) {
        return true
      }
    }
    false
  }

  def asInt(iField: Int): Int = {
    if (iField >= 0 && iField < table.nFields) {
      values(iField).asInt()
    } else 0
  }

  def asDouble(iField: Int): Double = {
    if (iField >= 0 && iField < table.nFields) {
      values(iField).asDouble()
    } else 0.0
  }

}

object TileTableRecord {
  def create(pTable: TileTable, index: Int): TileTableRecord = {
    val result: TileTableRecord = new TileTableRecord()
    result.table = pTable
    result.index = index
    result.flags = 0
    if (pTable != null && pTable.nFields > 0) {
      result.values = new Array[TileTableValue](pTable.nFields)
      for (iField <- 0 until pTable.nFields) {
        result.values(iField) = result.createValue(pTable.getFieldType(iField))
      }
    } else {
      result.values = null
    }

    result
  }
}
