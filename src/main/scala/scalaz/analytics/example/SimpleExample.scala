package scalaz.analytics.example

import scalaz.analytics.local._

/**
 * This is a temporary example to show a basic example of a usage.
 */
object SimpleExample {

  def main(args: Array[String]): Unit = {
    println(ds1)
    println(ds2)
    println(tupleExample)
    println(tupleExample2)
  }

  val ds1: DataSet[Int] =
    empty[Int]
      .map(i => i * 7)
      .distinct

  val ds2: DataStream[Int] =
    emptyStream[Int]
      .filter(i => i + 1 > 0)
      .distinct(Window.FixedTimeWindow())

  val tupleExample: DataSet[(Int, Boolean)] =
    empty[(Int, String)]
      .map(_ => (4, false))
      .filter(_._2)

  val tupleExample2: DataSet[(Int, String)] =
    empty[(Int, String)]
      .map(_ => (4, false)) // tuple of literals works
      .map(s => (3, s._1)) // tuple with right side projection
      .map(s => (s._2, "")) // tuple with left side projection
}
