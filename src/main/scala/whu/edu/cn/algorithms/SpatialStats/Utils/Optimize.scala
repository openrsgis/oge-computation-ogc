package whu.edu.cn.algorithms.SpatialStats.Utils

import breeze.linalg.{DenseVector, _}

import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.util.control.Breaks

/**
 * 提供参数优化的计算方法：黄金分割法和Nelder-Mead算法
 */
object Optimize {

  /**
   *
   * @param lower    优化值的下限
   * @param upper    优化值的上限
   * @param eps      优化条件，迭代相差小于eps时退出
   * @param findMax  寻找最大值为true，寻找最小值为false，默认为true
   * @param function 获取（更新）优化值的函数，需要为输入double，输出double的类型
   * @return 优化结果
   */
  def goldenSelection(lower: Double, upper: Double, eps: Double = 1e-10, findMax: Boolean = true, function: Double => Double):
  (Double, Array[Double], Array[Double], Array[Double]) = {
    var iter: Int = 0
    val max_iter = 1000
    val loop = new Breaks
    val ratio: Double = (sqrt(5) - 1) / 2.0
    var a = lower + 1e-12
    var b = upper - 1e-12
    var step = b - a
    var p = a + (1 - ratio) * step
    var q = a + ratio * step
    var f_a = function(a)
    var f_b = function(b)
    var f_p = function(p)
    var f_q = function(q)
    val opt_iter = new ArrayBuffer[Double]()
    val opt_val = new ArrayBuffer[Double]()
    val opt_res = new ArrayBuffer[Double]()
    //    println(f_a,f_b,f_p,f_q)
    loop.breakable {
      while (abs(f_a - f_b) >= eps && iter < max_iter) {
        if (findMax) {
          if (f_p > f_q) {
            b = q
            f_b = f_q
            q = p
            f_q = f_p
            step = b - a
            p = a + (1 - ratio) * step
            f_p = function(p)
          } else {
            a = p
            f_a = f_p
            p = q
            f_p = f_q
            step = b - a
            q = a + ratio * step
            f_q = function(q)
          }
        }
        else {
          if (f_p < f_q) {
            b = q
            f_b = f_q
            q = p
            f_q = f_p
            step = b - a
            p = a + (1 - ratio) * step
            f_p = function(p)
          } else {
            a = p
            f_a = f_p
            p = q
            f_p = f_q
            step = b - a
            q = a + ratio * step
            f_q = function(q)
          }
        }
        iter += 1
        opt_iter += iter
        //        opt_val += (b + a) / 2.0
        //        opt_res += function(sc, (b + a) / 2.0)
        opt_val += p
        opt_res += f_p
        //        println(s"Iter: $iter, optimize value: $p, result is $f_p")
        if (abs(a - b) < eps / 10) {
          loop.break()
        }
      }
    }
    opt_iter += (iter + 1)
    opt_val += (b + a) / 2.0
    opt_res += function((b + a) / 2.0)
    //    println((b + a) / 2.0, function((b + a) / 2.0))
    //    ((b + a) / 2.0, opt_iter.toArray, opt_val.toArray)
    ((b + a) / 2.0, opt_iter.toArray, opt_val.toArray, opt_res.toArray)
  }


  def nelderMead(optparams: Array[Double], function: (Array[Double]) => Double): Array[Double] = {
    var iter = 0
    val max_iter = 1000
    val th_eps = 1e-7 //sqrt的结果，不开根号是1e-15的级别
    // 如果是double和densevector的输入（rho: Double, betas: DenseVector[Double]，function也是以这两个为输入）
    //    val optdata: Array[Array[Double]] = Array(Array(rho), betas.toArray)
    //    val optParameter = optdata.flatten
    val optParameter = optparams.clone()

    //如果是3维，算上初始点一共3+1个点，m=3。但是，又因为点数从0开始算，m作为下标，实际应该是3-1=2
    val m = optParameter.length - 1

    var optArr = new ArrayBuffer[Array[Double]]
    optArr += optParameter //先放入没有变化的，第0个
    for (i <- 0 until optParameter.length) {
      val tmp = optParameter.clone()
      tmp(i) = tmp(i) * 1.05
      optArr += tmp
    }
    //    optArr.map(t=>t.foreach(println))
    //存储四个点的优化目标函数值，0代表第一次，只用一次。
    //    val re_lagsse0 = optArr.toArray.map(t => function(t(0), DenseVector(t.drop(1))))//用这种方式来把第一个元素和后面的分开
    val re_lagsse0 = optArr.toArray.map(t => function(t))
    var arr_lagsse = re_lagsse0.clone()
    var eps = 1.0
    //这个是所有的arr
    var ord_Arr = optArr.clone()

    while (eps >= th_eps && iter < max_iter) {
      //排序，从小到大
      val re_lagsse_idx = arr_lagsse.zipWithIndex.sorted
      //      re_lagsse_idx.foreach(println)
      val ord_0 = ord_Arr(re_lagsse_idx(0)._2)
      val ord_m = ord_Arr(re_lagsse_idx(m)._2)
      val ord_m1 = ord_Arr(re_lagsse_idx(m + 1)._2)
      //如果第m+1的点需要改变，这个是为了放进数组里
      var ord_m1_change = ord_m1.clone()
      //      ord_m1.foreach(println)
      val lagsse_0 = function(ord_0)
      val lagsse_m = function(ord_m)
      val lagsse_m1 = function(ord_m1)
      //求点0和m+1的差
      val dif = DenseVector(ord_m1) - DenseVector(ord_0)
      eps = sqrt(dif.toArray.map(t => t * t).sum)
      //      println(s"the iter is $iter, the difference is $dif, the eps is $eps")

      //这个是前m个的arr，从0到m
      var ord_0mArr = new ArrayBuffer[Array[Double]]
      for (i <- 0 until m + 1) {
        val tmp = ord_Arr(re_lagsse_idx(i)._2)
        ord_0mArr += tmp
      }
      //这个是1到m+1个的arr,不包括第0
      var ord_1m1Arr = new ArrayBuffer[Array[Double]]
      for (i <- 1 until m + 1 + 1) {
        val tmp = ord_Arr(re_lagsse_idx(i)._2)
        ord_1m1Arr += tmp
      }
      var flag_A10: Boolean = false
      //质心c
      val c = nm_gravityCenter(ord_0mArr.toArray)
      //反射点r
      val r = (DenseVector(c) + 1.0 * (DenseVector(c) - DenseVector(ord_m1))).toArray
      val lagsse_r = function(r) //计算r点的sse

      ord_Arr.clear()
      ord_Arr = ord_0mArr.clone() //前m个点已经放进来了

      if (lagsse_r <= lagsse_0) {
        //拓展点s
        val s = (DenseVector(c) + 2.0 * (DenseVector(c) - DenseVector(ord_m1))).toArray
        val lagsse_s = function(s)
        if (lagsse_s <= lagsse_r) {
          ord_m1_change = s
        } else {
          ord_m1_change = r
        }
      }
      else if (lagsse_r > lagsse_0 && lagsse_r <= lagsse_m) {
        ord_m1_change = r
      }
      else if (lagsse_r > lagsse_m && lagsse_r <= lagsse_m1) {
        val e1 = (DenseVector(c) + (DenseVector(r) - DenseVector(c)) * 0.5).toArray
        val lagsse_e1 = function(e1)
        if (lagsse_e1 <= lagsse_r) {
          ord_m1_change = e1
        } else {
          val ord_1m1vec = ord_1m1Arr.map(t => DenseVector(t))
          val v = ord_1m1vec.map(t => t + (t - DenseVector(ord_0)) * 0.5)
          ord_Arr.clear()
          ord_Arr += ord_0
          val v_arr = v.map(t => t.toArray).toArray
          for (i <- 0 until v_arr.length) {
            ord_Arr += v_arr(i)
          }
          flag_A10 = true
        }
      }
      else if (lagsse_r > lagsse_m1) {
        val e2 = (DenseVector(c) + (DenseVector(ord_m1) - DenseVector(c)) * 0.5).toArray
        val lagsse_e2 = function(e2)
        if (lagsse_e2 <= lagsse_m1) {
          ord_m1_change = e2
        } else {
          val ord_1m1vec = ord_1m1Arr.map(t => DenseVector(t))
          val v = ord_1m1vec.map(t => t + (t - DenseVector(ord_0)) * 0.5)
          ord_Arr.clear()
          ord_Arr += ord_0
          val v_arr = v.map(t => t.toArray).toArray
          for (i <- 0 until v_arr.length) {
            ord_Arr += v_arr(i)
          }
          flag_A10 = true
        }
      }
      else {
        val ord_1m1vec = ord_1m1Arr.map(t => DenseVector(t))
        val v = ord_1m1vec.map(t => t + (t - DenseVector(ord_0)) * 0.5)
        ord_Arr.clear()
        ord_Arr += ord_0
        val v_arr = v.map(t => t.toArray).toArray
        for (i <- 0 until v_arr.length) {
          ord_Arr += v_arr(i)
        }
        flag_A10 = true
      }
      // 一般情况下需要更新m+1，把m+1放入arr种
      if (!flag_A10) {
        ord_Arr += ord_m1_change
      }
      //      ord_Arr.map(t => t.foreach(println))
      //      println(flag_A10)
      //更新sse的值
      arr_lagsse = ord_Arr.toArray.map(t => function(t))
      //      arr_lagsse.foreach(println)
      //      println(lagsse_r <= lagsse_0 ,lagsse_r > lagsse_0 && lagsse_r <= lagsse_m, lagsse_r > lagsse_m && lagsse_r <= lagsse_m1, lagsse_r > lagsse_m1)
      iter += 1
    }
    //    println("-------result--------")
    //    ord_Arr.map(t => t.foreach(println))
    //    println("----------input----------")
    //    optParameter.foreach(println)
    //    println("----------Nelder-Mead optimize result----------")
    //    ord_Arr(0).foreach(println)
    ord_Arr(0)
  }

  def nm_gravityCenter(calArr: Array[Array[Double]]): Array[Double] = {
    val flat = calArr.flatMap(t => t.zipWithIndex)
    //    flat.foreach(println)
    val re = new Array[Double](calArr(0).length)
    for (i <- 0 until re.length) {
      val tmp = flat.filter(t => t._2 == i).map(t => t._1)
      re(i) = tmp.sum / tmp.length
    }
    //    re.foreach(println)
    re
  }

}
