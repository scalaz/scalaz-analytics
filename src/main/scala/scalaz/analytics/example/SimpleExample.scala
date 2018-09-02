package scalaz.analytics.example

import scalaz.analytics.local._

/**
 * This is a temporary example to show a basic example of a usage.
 */
object SimpleExample {

  def main(args: Array[String]): Unit = {
    println(ds1)
    println(ds2)
  }

  val ds1: Dataset[Int] =
    empty[Int]
      .map(i => i * 7)
      .distinct

  val ds2: DataStream[Int] =
    emptyStream[Int]
      .filter(i => i + 1 > 0)
      .distinct(Window.FixedTimeWindow())
}
