package whu.edu.cn.algorithms.terrain.core

import scala.util.control.Breaks.{break, breakable}

abstract class TileIndexCompare {
  def compare(a: Int, b: Int): Int
}

class TileIndex {
  var nValues: Int = _
  var index: Array[Int] = new Array[Int](0)
  var bProgress: Boolean = false

  def create(nvalues: Int, pCompare: TileIndexCompare): Boolean = {
    if (pCompare != null && setArray(nvalues) && setIndex(pCompare)) {
      return true
    }
    nValues = 0
    index = new Array[Int](0)
    false
  }

  def setArray(nvalues: Int): Boolean = {
    if (nvalues < 1) {
      nValues = 0
      index = new Array[Int](0)
      return true
    }

    if (nvalues == nValues) {
      return true
    }

    if (nValues > nvalues) {
      var i: Int = 0
      var j: Int = nvalues
      while (i < nvalues && j < nValues) {
        if (index(i) >= nvalues) {
          while (index(j) >= nvalues) {
            j += 1
            if (j >= nValues) {
              return false
            }
          }
          val c: Int = index(i)
          index(i) = index(j)
          index(j) = c
        }
        i += 1
      }
    }

    val Index: Array[Int] = index ++ Array.ofDim[Int](nvalues)
    if (Index == null) { return false }
    index = Index
    if (nValues < nvalues) {
      for (i <- nValues until nvalues) {
        index(i) = i
      }
    }
    nValues = nvalues
    true

  }

  def swapIndex(i: Int, j: Int): Unit = {
    val tmp = index(i)
    index(i) = index(j)
    index(j) = tmp
  }

  def setIndex(pCompare: TileIndexCompare): Boolean = {
    val M: Int = 7
    var indxt, itemp, i, j, k, a: Int = 0
    var l: Int = 0
    var ir: Int = nValues - 1
    var nstack: Int = 64
    var jstack: Int = 0

    val istack: TileArrayInt = TileArrayInt.create(nstack)
    var nProcessed: Int = 0

    breakable {
      while (true) {
        if (ir - l < M) {
          if (bProgress) {
            return false
          }

          j = l + 1
          while (j <= ir) {
            a = index(j)
            indxt = index(j)
            breakable {
              i = j - 1
              while (i >= 0) {
                if (pCompare.compare(index(i), a) <= 0) {
                  break
                }
                index(i + 1) = index(i)
                i -= 1
              }
            }
            index(i + 1) = indxt
            j += 1
          }

          if (jstack == 0) {
            break
          }
          ir = istack(jstack)
          jstack -= 1
          l = istack(jstack)
          jstack -= 1
        } else {
          k = (l + ir) / 2
          swapIndex(k, l + 1)

          if (pCompare.compare(index(l + 1), index(ir)) > 0) {
            swapIndex(l + 1, ir)
          }
          if (pCompare.compare(index(l), index(ir)) > 0) {
            swapIndex(l, ir)
          }
          if (pCompare.compare(index(l + 1), index(l)) > 0) {
            swapIndex(l + 1, l)
          }

          i = l + 1
          j = ir
          a = index(l)
          indxt = index(l)

          breakable {
            while (true) {
              do i += 1 while (pCompare.compare(index(i), a) < 0)
              do j -= 1 while (pCompare.compare(index(j), a) > 0)
              if (j < i) break
              swapIndex(i, j)
            }
          }

          index(l) = index(j)
          index(j) = indxt
          jstack += 2

          if (jstack >= nstack) {
            nstack += 64
            istack.setArray(nstack)
          }

          if (ir - i + 1 >= j - l) {
            istack(jstack) = ir
            istack(jstack - 1) = i
            ir = j - 1
          } else {
            istack(jstack) = j - 1
            istack(jstack - 1) = l
            l = i
          }
        }
      }
    }
    true
  }

  def apply(Position: Int): Int = {
    if (Position < 0 || Position >= nValues) {
      -1
    } else {
      index(Position)
    }
  }

}
