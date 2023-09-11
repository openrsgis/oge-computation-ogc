package whu.edu.cn.algorithms.terrain.core

class TileTable {
  var nFields: Int = 0
  var nRecords: Int = 0
  var nBuffer: Int = 0

  var fieldName: Array[String] = new Array[String](0)
  var fieldType: Array[DataType] = new Array[DataType](0)
  var records: Array[TileTableRecord] = new Array[TileTableRecord](0)
  var index: TileIndex = new TileIndex()
  var indexFields: TileArrayInt = new TileArrayInt()

  def getFieldName(iField: Int): String = {
    if (iField >= 0 && iField < nFields) {
      fieldName(iField)
    } else {
      null
    }
  }

  def getFieldType(iField: Int): DataType = {
    if (iField >= 0 && iField < nFields) {
      fieldType(iField)
    } else {
      DATATYPEUndefined
    }
  }

  def addField(Name: String, Type: DataType, Position: Int = -1): Boolean = {
    var position: Int = Position
    if (Position < 0 || Position > nFields) {
      position = nFields
    }

    nFields += 1
    fieldName ++= Array.ofDim[String](nFields - fieldName.length)
    fieldType ++= Array.ofDim[DataType](nFields - fieldType.length)

    for (i <- nFields - 1 until position by -1) {
      fieldName(i) = fieldName(i - 1)
      fieldType(i) = fieldType(i - 1)
    }

    fieldName(position) = Name
    fieldType(position) = Type
    true
  }

  def addRecord(Copy: TileTableRecord = null): TileTableRecord = {
    insRecord(nRecords, Copy)
  }

  def insRecord(iRecord: Int, Copy: TileTableRecord = null): TileTableRecord = {
    var irecords: Int = iRecord
    if (irecords < 0) {
      irecords = 0
    } else if (irecords > nRecords) {
      irecords = nRecords
    }
    var pRecord: TileTableRecord = null
    if (incArray()) {
      pRecord = getNewRecord(nRecords)
    } else {
      pRecord = null
    }

    if (pRecord != null) {
      if (Copy != null) {
        pRecord.assign(Copy)
      }
      records(irecords) = pRecord
      nRecords += 1
    }
    pRecord

  }

  def incArray(): Boolean = {
    if (nRecords < nBuffer) {
      return true
    }
    val pRecords: Array[TileTableRecord] =
      records ++ Array.ofDim[TileTableRecord](nBuffer + getGrowSize(nBuffer))

    if (pRecords == null) {
      return false
    }

    records = pRecords
    nBuffer += getGrowSize(nBuffer)
    true
  }

  def getNewRecord(index: Int): TileTableRecord = {
    TileTableRecord.create(this, index)
  }

  def getGrowSize(n: Int): Int = {
    if (n < 256) {
      1
    } else {
      if (n < 8192) {
        128
      } else {
        1024
      }
    }
  }

  def setIndex(
      Field1: Int,
      Order1: TableIndexOrder,
      Field2: Int = -1,
      Order2: TableIndexOrder = TABLEINDEXNone,
      Field3: Int = -1,
      Order3: TableIndexOrder = TABLEINDEXNone
  ): Boolean = {
    indexFields.array.nBuffer = 0
    indexFields.array.nValues = 0
    if (Field1 >= 0 && Field1 < nFields && Order1 != TABLEINDEXNone) {
      val field1: Int = Field1 + 1
      if (Order1 == TABLEINDEXAscending) {
        indexFields += field1
      } else {
        indexFields += -field1

        if (Field2 >= 0 && Field2 < nFields && Order2 != TABLEINDEXNone) {
          val field2: Int = Field2 + 1
          if (Order2 == TABLEINDEXAscending) {
            indexFields += field2
          } else {
            indexFields += -field2
          }

          if (Field3 >= 0 && Field3 < nFields && Order3 != TABLEINDEXNone) {
            val field3: Int = Field3 + 1
            if (Order3 == TABLEINDEXAscending) {
              indexFields += field3
            } else {
              indexFields += -field3
            }
          }
        }
      }
      indexUpdate()
    } else {
      delIndex()
    }
    isIndexed
  }

  def indexUpdate(): Unit = {
    val fields: TileArrayInt = TileArrayInt.create()
    var ascending: Boolean = false
    if (getIndexOrder(0) != TABLEINDEXDescenging) ascending = true
    for (i <- 0 until indexFields.getSize) {
      val field: Int = math.abs(indexFields(i)) - 1
      if (ascending) {
        if (indexFields(i) > 0) {
          fields += field
        } else {
          fields += -field
        }
      } else {
        if (indexFields(i) < 0) {
          fields += field
        } else {
          fields += -field
        }
      }
    }
    if (fields.getSize < 1 || !setIndex(index, fields, ascending)) {
      delIndex()
    }

  }

  def setIndex(
      index: TileIndex,
      fields: TileArrayInt,
      bAscending: Boolean
  ): Boolean =
    setIndex(index, fields.getArray, fields.getSize, bAscending)

  def setIndex(
      index: TileIndex,
      fields: Array[Int],
      nFields: Int,
      bAscending: Boolean
  ): Boolean = {
    val compare: TileTableRecordCompareFields =
      TileTableRecordCompareFields(this, fields, nFields, bAscending)
    compare.isOkay && index.create(nRecords, compare)
  }

  def getIndexOrder(i: Int): TableIndexOrder = {
    if (i >= indexFields.getSize) {
      TABLEINDEXNone
    } else if (indexFields(i) > 0) {
      TABLEINDEXAscending
    } else {
      TABLEINDEXDescenging
    }
  }

  def getRecord(index: Int): TileTableRecord = {
    if (index >= 0 && index < nRecords) {
      records(index)
    } else null
  }

  def getRecordByIndex(Index: Int): TileTableRecord = {
    if (Index >= 0 && Index < nRecords) {
      if (isIndexed) {
        getRecord(index(Index))
      } else {
        getRecord(Index)
      }
    } else null
  }

  def delIndex(): Boolean = {
    index.nValues = 0
    index.index = new Array[Int](0)
    indexFields.array.nBuffer = 0
    indexFields.array.nValues = 0
    true
  }

  def isIndexed: Boolean = nRecords > 0 && index.nValues == nRecords

  def apply(Index: Int): TileTableRecord = getRecordByIndex(Index)

}

object TileTable {

  def create(template: TileTable): TileTable = {
    if (template == null || template.nFields < 1) {
      return new TileTable()
    }
    val result: TileTable = new TileTable()
    for (i <- 0 until template.nFields) {
      result.addField(template.getFieldName(i), template.getFieldType(i))
    }
    result
  }

}
