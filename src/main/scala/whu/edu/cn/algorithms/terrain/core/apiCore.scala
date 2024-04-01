package whu.edu.cn.algorithms.terrain.core

sealed trait DataType
case object DATATYPEBit extends DataType
case object DATATYPEChar extends DataType
case object DATATYPEWord extends DataType
case object DATATYPEShort extends DataType
case object DATATYPEDword extends DataType
case object DATATYPEInt extends DataType
case object DATATYPEULong extends DataType
case object DATATYPELong extends DataType
case object DATATYPEFloat extends DataType
case object DATATYPEDouble extends DataType
case object DATATYPEString extends DataType
case object DATATYPEDate extends DataType
case object DATATYPEColor extends DataType
case object DATATYPEBinary extends DataType
case object DATATYPEUndefined extends DataType

sealed trait DistanceWeighting
case object DISTWGHTNone extends DistanceWeighting
case object DISTWGHTIDW extends DistanceWeighting
case object DISTWGHTEXP extends DistanceWeighting
case object DISTWGHTGAUSS extends DistanceWeighting

sealed trait TableIndexOrder
case object TABLEINDEXNone extends TableIndexOrder
case object TABLEINDEXAscending extends TableIndexOrder
case object TABLEINDEXDescenging extends TableIndexOrder

sealed trait ArrayGrowth
case object ARRAYGROWTH0 extends ArrayGrowth
case object ARRAYGROWTH1 extends ArrayGrowth
case object ARRAYGROWTH2 extends ArrayGrowth
case object ARRAYGROWTH3 extends ArrayGrowth

sealed trait NODETYPE
case object NODESPRING extends NODETYPE
case object NODEJUNCTION extends NODETYPE
case object NODEOUTLET extends NODETYPE
case object NODEMOUTH extends NODETYPE
