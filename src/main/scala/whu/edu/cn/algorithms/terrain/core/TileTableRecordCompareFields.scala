package whu.edu.cn.algorithms.terrain.core

case class TileTableRecordCompareFields(
    pTable: TileTable,
    fields: Array[Int],
    nFields: Int,
    ascending: Boolean
) extends TileIndexCompare {
  def isOkay: Boolean = pTable != null

  override def compare(_a: Int, _b: Int): Int = {
    var Difference: Int = 0
    var i: Int = 0
    while (Difference == 0 && i < nFields) {
      var Field: Int = fields(i)
      var Ascending: Boolean = false
      if (i == 0) {
        Ascending = ascending
      } else if (ascending) {
        Ascending = Field > 0
      } else Ascending = Field < 0

      var a: Int = 0
      var b: Int = 0
      if (Ascending) {
        a = _a
        b = _b
      } else {
        a = _b
        b = _a
      }
      Field = math.abs(Field)
      pTable.getFieldType(Field) match {
        case _ =>
          val d: Double = pTable.getRecord(a).asDouble(Field) - pTable
            .getRecord(b)
            .asDouble(Field)
          if (d < 0) {
            Difference = -1
          } else if (d > 0) {
            Difference = 1
          } else Difference = 0
      }
      i += 1
    }
    Difference
  }
}
