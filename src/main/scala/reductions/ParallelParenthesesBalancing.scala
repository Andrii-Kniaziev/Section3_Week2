package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def check(chrs: Array[Char], balance: Int): Boolean =
      if (balance < 0) false
      else if (chrs.isEmpty) balance == 0
      else if (chrs.head == '(') check(chrs.tail, balance + 1)
      else if (chrs.head == ')') check(chrs.tail, balance - 1)
      else check(chrs.tail, balance)

    check(chars, 0)
  }



  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      var waitClosing = false
      var res = (arg1, arg2)
      def accumul(char: Char, acc: (Int, Int)): (Int, Int) = {
        if(char == '(')
          waitClosing = true
          (acc._1 + 1, acc._2)
        else if(waitClosing && char == ')') (acc._1 - 1, acc._2)
        else if(char == ')') (acc._1, acc._2 + 1)
        else acc
      }

      for(i <- idx until until)
        res = accumul(chars(i), res)

      res
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if(until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = from + (until - from) / 2
        val (pair1, pair2) = parallel(reduce(from, mid), reduce(mid, until))

        if(pair1._1 + pair2._2 >= pair1._2 + pair2._1) (pair1._1 + pair2._1, pair1._2 + pair2._2)
        else (Int.MinValue, pair1._1)
      }
    }

    val res = reduce(0, chars.length)
    res._1 + res._2 == 0
  }
  
