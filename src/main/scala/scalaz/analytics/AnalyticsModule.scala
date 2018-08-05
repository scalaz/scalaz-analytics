package scalaz.analytics

import scalaz.zio.IO

trait AnalyticsModule {
  type Dataset[_]

  type Type[_]
  type NumericType[A] <: Type[A]

  implicit val intNumeric: NumericType[Int]
  implicit val longNumeric: NumericType[Long]
  implicit val floatNumeric: NumericType[Float]
  implicit val doubleNumeric: NumericType[Double]
  implicit def tuple2Type[A: Type, B: Type]: Type[(A, B)]

  /**
  * Add all kleisli operations to this (and in the standard library)*/
  trait =>: [-A, +B] { self =>

    def >>> [C: Type] (that: B =>: C): A =>: C = stdLib.compose(that, self)
    def <<< [C: Type] (that: C =>: A): C =>: B = that >>> self

    def &&& [C: Type] (that: A =>: C): A =>: (B, C) = stdLib.fanOut(self, that)
  }

  /**
    * Add all set operations to this.
    */
  trait SetOperations {
    def empty[A: Type]: Dataset[A]

    def union[A: Type](l: Dataset[A], r: Dataset[A]): Dataset[A]

    def intersect[A: Type](l: Dataset[A], r: Dataset[A]): Dataset[A]

    def except[A: Type](l: Dataset[A], r: Dataset[A]): Dataset[A]

    def distinct[A: Type](d: Dataset[A]): Dataset[A]

    def map[A: Type, B: Type](d: Dataset[A])(f: A =>: B): Dataset[B]

    def sort[A: Type](d: Dataset[A]): Dataset[A]

    def distinctBy[A: Type, B: Type](d: Dataset[A])(by: A =>: B): Dataset[A]
  }

  trait StdLib {
    def id[A: Type]: A =>: A

    def compose[A: Type, B: Type, C: Type](f: B =>: C, g: A =>: B): A =>: C

    def mult[A: NumericType]: (A, A) =>: A

    def sum[A: NumericType]: (A, A) =>: A

    def diff[A: NumericType]: (A, A) =>: A

    def fanOut[A: Type, B: Type, C: Type](fst: A =>: B, snd: A =>: C): A =>: (B, C)

    def int(v: Int): Any =>: Int
  }

  implicit class NumericSyntax[A, B](val l: A =>: B) extends AnyVal {
    def * (r: A =>: B)(implicit B: NumericType): A =>: B =
      (l &&& r) >>> stdLib.mult

    def + (r: A =>: B)(implicit B: NumericType): A =>: B =
      (l &&& r) >>> stdLib.sum

    def - (r: A =>: B)(implicit B: NumericType): A =>: B =
      (l &&& r) >>> stdLib.diff
  }

  implicit class DatasetSyntax[A](d: Dataset[A]) {
    def distinctBy[A: Type, B: Type](f: (A =>: A) => (A =>: B)): Dataset[A] =
      setOps.distinctBy(d)(f(stdLib.id))
  }

  val setOps: SetOperations
  val stdLib: StdLib

  def run[A: Type](d: Dataset[A]): IO[Error, Seq[A]]
}
