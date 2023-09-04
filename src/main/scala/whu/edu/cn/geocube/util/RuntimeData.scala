package whu.edu.cn.geocube.util

/**
 * A demo class for using multi-threads technology to
 * perform sum operation on an array.
 */
class RuntimeData {
  def apply(unit: Unit): Unit = {}

  //sum result
  var sum:Int = 0

  //default threads num
  var defThreadCount:Int = 5

  //a counter for finished threads
  var finishThreadCount:Int = 0

  /**
   * Constructer
   *
   * @param _sum
   * @param _defThreadCount
   * @param _finishThreadCount
   */
  def this(_sum: Int, _defThreadCount:Int, _finishThreadCount:Int)= {
    this()
    sum = _sum
    defThreadCount = _defThreadCount
    finishThreadCount = _finishThreadCount
  }

  /**
   * Calculate the number of needed threads by the
   * number of elements to be processed
   *
   * @param array an array of elements to be processed
   * @return
   */
  def getThreadCount(array: Array[Int]): Int = {
    if (array.length < defThreadCount) return array.length
    defThreadCount
  }

}

object RuntimeData{
  /**
   * Using multi-threads technology to perform sum operation on an array.
   *
   * @param array
   * @return
   */
  def sum(array: Array[Int]): Int = {
    val rd = new RuntimeData(0, 5, 0)
    //get applied threads
    val threadCount = rd.getThreadCount(array)
    //calculate the number of elements assigned for each thread
    val lenPerThread = array.length / threadCount
    //Perform sum operation using multi-threads technology
    for(i <- 0 until threadCount){
      val index = i
      new Thread(){
        override def run(): Unit = {
          var s:Int = 0
          val start = index * lenPerThread
          val end = start + lenPerThread
          for(j <- start until end) s += array(j)
          println("Thread " + Thread.currentThread() + "is running...")
          synchronized(rd){
            rd.sum += s
            rd.finishThreadCount += 1
          }
        }
      }.start()
    }

    //barrier
    while (rd.finishThreadCount != threadCount) {
      try {
        Thread.sleep(1);
      }catch {
        case ex: InterruptedException  => {
          ex.printStackTrace()
          System.err.println("exception===>: ...")
        }
      }
    }
    rd.sum
  }

  def main(args: Array[String]): Unit = {
    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    var sum = 0
    val serialStart = System.currentTimeMillis()
    array.foreach(sum += _)
    val serialEnd = System.currentTimeMillis()
    println(sum + ":" + (serialEnd - serialStart))

    val parallelStart = System.currentTimeMillis()
    sum = RuntimeData.sum(array)
    val parallelEnd = System.currentTimeMillis()
    println(sum + ":" + (parallelEnd - parallelStart))

  }

}
