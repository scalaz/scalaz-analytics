package scalaz.analytics.example

import scalaz.analytics.local._

/**
 * This is a temporary example to show a basic example of a usage.
 */
object SimpleExample {

  def main(args: Array[String]): Unit =
    println(ds)

  val ds: DataStream[Int] =
    empty[Int]
      .map(i => i * 7)
      .distinctBy(i => i % 2)
}
