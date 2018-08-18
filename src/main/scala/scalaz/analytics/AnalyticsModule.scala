package scalaz.analytics

import scalaz.zio.IO


/**
  * An analytics module defined the abstract components required to build a reified program description.
  */
trait AnalyticsModule {

  /**
    * An abstract DataStream.
    */
  type DataStream[_]

  /**
    * The set of types that are representable in this module.
    */
  type Type[_]

  /**
    * The subset of types which can be viewed as NumericType.
    */
  type NumericType[A]

  /**
    * A type that represents an Unknown Schema for some Data.
    */
  type Unknown

  /**
    * Proofs that the various types are [[Type]]s or [[NumericType]]s.
    */
  implicit val unknown: Type[Unknown]
  implicit val intNumeric: NumericType[Int]
  implicit val longNumeric: NumericType[Long]
  implicit val floatNumeric: NumericType[Float]
  implicit val doubleNumeric: NumericType[Double]
  implicit def tuple2Type[A: Type, B: Type]: Type[(A, B)]

  /**
    * Allows us to prove that anytime we have a NumericType we also have a Type
    */
  implicit def numericType[A: NumericType]: Type[A]

  /**
    * An Arrow between A and B.
    * Arrows allow us to define abstract pipelines in scalaz-analytics.
    */
  type =>:[-A, +B]

  /**
    * Syntax a user can use to build their pipelines
    */
  implicit class FunctionSyntax[A, B](self: A =>: B) {
    def andThen[C: Type](that: B =>: C)(implicit typeA: Type[A], typeB: Type[B]): A =>: C = stdLib.compose(that, self)
    def >>>[C: Type](that: B =>: C)(implicit typeA: Type[A], typeB: Type[B]): A =>: C = andThen(that)

    def compose[C: Type](that: C =>: A)(implicit typeA: Type[A], typeB: Type[B]): C =>: B = stdLib.compose(self, that)
    def <<<[C: Type](that: C =>: A)(implicit typeA: Type[A], typeB: Type[B]): C =>: B = compose(that)

    def combine[C: Type](that: A =>: C)(implicit typeA: Type[A], typeB: Type[B]): A =>: (B, C) = stdLib.fanOut(self, that)
    def &&&[C: Type](that: A =>: C)(implicit typeA: Type[A], typeB: Type[B]): A =>: (B, C) = stdLib.fanOut(self, that)
  }

  /**
    * The Operations supported by the core DataStream abstraction in scalaz-analytics.
    */
  trait SetOperations {
    def empty[A: Type]: DataStream[A]

    def union[A: Type](l: DataStream[A], r: DataStream[A]): DataStream[A]

    def intersect[A: Type](l: DataStream[A], r: DataStream[A]): DataStream[A]

    def except[A: Type](l: DataStream[A], r: DataStream[A]): DataStream[A]

    def distinct[A: Type](d: DataStream[A]): DataStream[A]

    def map[A: Type, B: Type](d: DataStream[A])(f: A =>: B): DataStream[B]

    def sort[A: Type](d: DataStream[A]): DataStream[A]

    def distinctBy[A: Type, B: Type](d: DataStream[A])(by: A =>: B): DataStream[A]
  }

  /**
    * The standard library of scalaz-analytics.
    */
  trait StandardLibrary {
    def id[A: Type]: A =>: A

    def compose[A: Type, B: Type, C: Type](f: B =>: C, g: A =>: B): A =>: C
    def andThen[A: Type, B: Type, C: Type](f: A =>: B, g: B =>: C): A =>: C

    def mult[A: NumericType]: (A, A) =>: A

    def sum[A: NumericType]: (A, A) =>: A

    def diff[A: NumericType]: (A, A) =>: A

    def mod[A: NumericType]: (A, A) =>: A

    def fanOut[A: Type, B: Type, C: Type](fst: A =>: B, snd: A =>: C): A =>: (B, C)

    def split[A: Type, B: Type, C: Type, D: Type](f: A =>: B, g: C =>: D): (A,  C) =>: (B, D)

    def product[A: Type, B: Type](fab: A =>: B): (A, A) =>: (B, B)

    def int[A: Type](v: scala.Int): A =>: Int
    def long[A: Type](v: scala.Long): A =>: Long
    def float[A: Type](v: scala.Float): A =>: Float
    def double[A: Type](v: scala.Double): A =>: Double
    def decimal[A: Type](v: scala.BigDecimal): A =>: BigDecimal
    def string[A: Type](v: scala.Predef.String): A =>: String

    def column[A: Type](str: scala.Predef.String): Unknown =>: A
  }

  /**
    * A DSL for numeric operations
    */
  implicit class NumericSyntax[A, B](val l: A =>: B) {
    def * (r: A =>: B)(implicit A: Type[A], B: NumericType[B]): A =>: B =
      (l combine r) andThen stdLib.mult

    def + (r: A =>: B)(implicit A: Type[A], B: NumericType[B]): A =>: B =
      (l &&& r) >>> stdLib.sum

    def - (r: A =>: B)(implicit A: Type[A], B: NumericType[B]): A =>: B =
      (l &&& r) >>> stdLib.diff

    def % (r: A =>: B)(implicit A: Type[A], B: NumericType[B]): A =>: B =
      (l &&& r) >>> stdLib.mod
  }

  /**
    * A DSL for building the DataStream data structure in a manner familiar to libraries like Spark/Flink etc
    */
  implicit class DataStreamSyntax[A](d: DataStream[A])(implicit A: Type[A]) {
    def map[B: Type](f: (A =>: A) => (A =>: B)): DataStream[B] =
      setOps.map(d)(f(stdLib.id))

    def distinctBy[B: Type](f: (A =>: A) => (A =>: B)): DataStream[A] =
      setOps.distinctBy(d)(f(stdLib.id))
  }

  val setOps: SetOperations
  val stdLib: StandardLibrary


  /**
    * Described a transformation from some Unknown Column type to a concrete Type
    */
  def column[A: Type](str: String): Unknown =>: A = stdLib.column(str)(implicitly[Type[A]])

  /**
    * Loads a [[DataStream]] without type information
    */
  def load(path: String): DataStream[Unknown]

  /**
    * Execute the [[DataStream]]
    */
  def run[A: Type](d: DataStream[A]): IO[Error, Seq[A]]
}
