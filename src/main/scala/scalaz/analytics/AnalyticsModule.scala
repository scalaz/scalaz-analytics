package scalaz.analytics

import scalaz.zio.IO

import scala.language.implicitConversions

/**
 * An analytics module defined the abstract components required to build a reified program description.
 */
trait AnalyticsModule {

  /**
   * An abstract DataSet
   */
  type DataSet[A]

  /**
   * An abstract DataStream.
   */
  type DataStream[A]

  /**
   * A window is a way to specify a view on a DataStream/DataSet
   */
  sealed trait Window

  object Window {
    case class FixedTimeWindow()   extends Window
    case class SlidingTimeWindow() extends Window
    case class SessionWindow()     extends Window
    // For internal use only
    case class GlobalWindow private () extends Window
  }

  /**
   * The set of types that are representable in this module.
   */
  type Type[_]

  object Type {
    def apply[A: Type]: Type[A] = implicitly[Type[A]]
  }

  /**
   * A type that represents an Unknown Schema for some Data.
   */
  type Unknown

  /**
   * Proofs that the various types are [[Type]]s.
   */
  implicit val unknown: Type[Unknown]

  implicit val intType: Type[Int]
  implicit val intNumeric: Numeric[Int]

  implicit val longType: Type[Long]
  implicit val longNumeric: Numeric[Long]

  implicit val floatType: Type[Float]
  implicit val floatNumeric: Numeric[Float]

  implicit val doubleType: Type[Double]
  implicit val doubleNumeric: Numeric[Double]

  implicit val decimalType: Type[BigDecimal]
  implicit val decimalNumeric: Numeric[BigDecimal]

  implicit val stringType: Type[String]
  implicit val booleanType: Type[Boolean]
  implicit val byteType: Type[Byte]
  implicit val nullType: Type[scala.Null]
  implicit val shortType: Type[Short]
  implicit val instantType: Type[java.time.Instant]
  implicit val dateType: Type[java.time.LocalDate]

  implicit def tuple2Type[A: Type, B: Type]: Type[(A, B)]

  /**
   * An Arrow between A and B.
   * Arrows allow us to define abstract pipelines in scalaz-analytics.
   */
  type =>:[-A, +B]

  /**
   * Syntax a user can use to build their pipelines
   */
  implicit class FunctionSyntax[A, B](self: A =>: B) {
    def andThen[C](that: B =>: C): A =>: C = stdLib.compose(that, self)
    def >>> [C](that: B =>: C): A =>: C    = andThen(that)

    def compose[C](that: C =>: A): C =>: B = stdLib.compose(self, that)
    def <<< [C](that: C =>: A): C =>: B    = compose(that)

    def combine[C](that: A =>: C): A =>: (B, C) = stdLib.fanOut(self, that)
    def &&& [C](that: A =>: C): A =>: (B, C)    = stdLib.fanOut(self, that)
  }

  /**
   * The DataSet/DataStream operations of scalaz-analytics
   */
  trait Ops[F[_]] {
    // Unbounded dataset ops
    // (These ops require only bounded space, even with unbounded data)
    def map[A, B](ds: F[A])(f: A =>: B): F[B]
    def flatMap[A, B](ds: F[A])(f: A =>: DataSet[B]): F[B]
    def filter[A](ds: F[A])(f: A =>: Boolean): F[A]
    // Streaming aggregations
    // (These ops produce cumulative results, some may require unbounded space)
    def scan[A, B](ds: F[A])(initial: Unit =>: B)(f: (B, A) =>: B): F[B]

    def scanAggregateBy[A, K, V](ds: F[A])(g: A =>: K)(initial: Unit =>: V)(
      f: (V, A) =>: V
    ): F[(K, V)]

    // Bounded dataset ops
    // (These ops can only be performed on bounded subsets of data)
    def fold[A, B](ds: F[A])(window: Window)(initial: Unit =>: B)(f: (B, A) =>: B): F[B]

    def aggregateBy[A, K, V](ds: F[A])(window: Window)(g: A =>: K)(initial: Unit =>: V)(
      f: (V, A) =>: V
    ): F[(K, V)]
    def distinct[A](ds: F[A])(window: Window): F[A]
  }

  /**
   * The standard library of scalaz-analytics.
   */
  trait StandardLibrary extends TupleLibrary with StringLibrary {
    def id[A: Type]: A =>: A
    def compose[A, B, C](f: B =>: C, g: A =>: B): A =>: C
    def andThen[A, B, C](f: A =>: B, g: B =>: C): A =>: C
    def fanOut[A, B, C](fst: A =>: B, snd: A =>: C): A =>: (B, C)
    def split[A, B, C, D](f: A =>: B, g: C =>: D): (A, C) =>: (B, D)
    def product[A, B](fab: A =>: B): (A, A) =>: (B, B)
  }

  trait TupleLibrary {
    def fst[A: Type, B]: (A, B) =>: A
    def snd[A, B: Type]: (A, B) =>: B
  }

  trait StringLibrary {
    def strSplit(pattern: String): String =>: DataSet[String]
    def strConcat: (String, String) =>: String
  }

  /**
   * A DSL for string operations
   */
  implicit class StringSyntax[A](val l: A =>: String) {
    def split(pattern: String): A =>: DataSet[String] = l >>> stdLib.strSplit(pattern)
    def concat(r: A =>: String): A =>: String         = (l &&& r) >>> stdLib.strConcat
  }

  trait Numeric[A] {
    def typeOf: Type[A] // underlying repr

    def mult: (A, A) =>: A
    def sum: (A, A) =>: A
    def diff: (A, A) =>: A
    def mod: (A, A) =>: A

    def greaterThan: (A, A) =>: Boolean
  }

  object Numeric {
    def apply[A: Numeric]: Numeric[A] = implicitly[Numeric[A]]
  }

  /**
   * A DSL for numeric operations
   */
  implicit class NumericSyntax[A, B](val l: A =>: B) {
    def * (r: A =>: B)(implicit B: Numeric[B]): A =>: B = (l &&& r) >>> B.mult
    def + (r: A =>: B)(implicit B: Numeric[B]): A =>: B = (l &&& r) >>> B.sum
    def - (r: A =>: B)(implicit B: Numeric[B]): A =>: B = (l &&& r) >>> B.diff
    def % (r: A =>: B)(implicit B: Numeric[B]): A =>: B = (l &&& r) >>> B.mod

    def > (r: A =>: B)(implicit B: Numeric[B]): A =>: Boolean = (l &&& r) >>> B.greaterThan
  }

  /**
   * A DSL for building the DataSet data structure
   */
  implicit class DataSetSyntax[A](ds: DataSet[A])(implicit A: Type[A]) {

    def map[B: Type](f: (A =>: A) => (A =>: B)): DataSet[B] =
      setOps.map(ds)(f(stdLib.id))

    def flatMap[B: Type](f: (A =>: A) => (A =>: DataSet[B])): DataSet[B] =
      setOps.flatMap(ds)(f(stdLib.id))

    def filter(f: (A =>: A) => (A =>: Boolean)): DataSet[A] =
      setOps.filter(ds)(f(stdLib.id))

    def scan[B: Type](initial: Unit =>: B)(f: ((B, A) =>: (B, A)) => ((B, A) =>: B)): DataSet[B] =
      setOps.scan(ds)(initial)(f(stdLib.id))

    def scanAggregate[V: Type](
      initial: Unit =>: V
    )(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataSet[(A, V)] =
      scanAggregateBy(identity)(initial)(f)

    def scanAggregateBy[K, V: Type](
      g: (A =>: A) => (A =>: K)
    )(initial: Unit =>: V)(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataSet[(K, V)] =
      setOps.scanAggregateBy(ds)(g(stdLib.id))(initial)(f(stdLib.id))

    def fold[B: Type](init: Unit =>: B)(f: ((B, A) =>: (B, A)) => ((B, A) =>: B)): DataSet[B] =
      setOps.fold(ds)(Window.GlobalWindow())(init)(f(stdLib.id))

    def aggregate[V: Type](
      initial: Unit =>: V
    )(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataSet[(A, V)] =
      aggregateBy(identity)(initial)(f)

    def aggregateBy[K, V: Type](
      g: (A =>: A) => (A =>: K)
    )(initial: Unit =>: V)(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataSet[(K, V)] =
      setOps.aggregateBy(ds)(Window.GlobalWindow())(g(stdLib.id))(initial)(f(stdLib.id))

    def distinct: DataSet[A] =
      setOps.distinct(ds)(Window.GlobalWindow())
  }

  /**
   * A DSL for building the DataStream data structure
   */
  implicit class DataStreamSyntax[A](ds: DataStream[A])(implicit A: Type[A]) {

    def map[B: Type](f: (A =>: A) => (A =>: B)): DataStream[B] =
      streamOps.map(ds)(f(stdLib.id))

    def flatMap[B: Type](f: (A =>: A) => (A =>: DataSet[B])): DataStream[B] =
      streamOps.flatMap(ds)(f(stdLib.id))

    def filter(f: (A =>: A) => (A =>: Boolean)): DataStream[A] =
      streamOps.filter(ds)(f(stdLib.id))

    def scan[B: Type](
      initial: Unit =>: B
    )(f: ((B, A) =>: (B, A)) => ((B, A) =>: B)): DataStream[B] =
      streamOps.scan(ds)(initial)(f(stdLib.id))

    def scanAggregate[V: Type](
      initial: Unit =>: V
    )(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataStream[(A, V)] =
      scanAggregateBy(identity)(initial)(f)

    def scanAggregateBy[K, V: Type](
      g: (A =>: A) => (A =>: K)
    )(initial: Unit =>: V)(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataStream[(K, V)] =
      streamOps.scanAggregateBy(ds)(g(stdLib.id))(initial)(f(stdLib.id))

    def fold[B: Type](
      window: Window
    )(init: Unit =>: B)(f: ((B, A) =>: (B, A)) => ((B, A) =>: B)): DataStream[B] =
      streamOps.fold(ds)(window)(init)(f(stdLib.id))

    def aggregate[V: Type](
      window: Window
    )(initial: Unit =>: V)(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataStream[(A, V)] =
      aggregateBy(window)(identity)(initial)(f)

    def aggregateBy[K, V: Type](window: Window)(
      g: (A =>: A) => (A =>: K)
    )(initial: Unit =>: V)(f: ((V, A) =>: (V, A)) => ((V, A) =>: V)): DataStream[(K, V)] =
      streamOps.aggregateBy(ds)(window)(g(stdLib.id))(initial)(f(stdLib.id))

    def distinct(window: Window): DataStream[A] =
      streamOps.distinct(ds)(window)
  }

  implicit class TupleSyntax[A, B, C](t: A =>: (B, C)) {

    def _1(implicit ev: Type[B]): A =>: B =
      stdLib.compose[A, (B, C), B](stdLib.fst, t)

    def _2(implicit ev: Type[C]): A =>: C =
      stdLib.compose[A, (B, C), C](stdLib.snd, t)
  }

  /**
   * Create an empty DataSet of type A
   */
  def empty[A: Type]: DataSet[A]

  /**
   * Create an empty DataStream of type A
   */
  def emptyStream[A: Type]: DataStream[A]

  // Entry points for various supported scala types into the Analytics Language
  implicit def int[A](v: scala.Int): A =>: Int
  implicit def long[A](v: scala.Long): A =>: Long
  implicit def float[A](v: scala.Float): A =>: Float
  implicit def double[A](v: scala.Double): A =>: Double
  implicit def decimal[A](v: scala.BigDecimal): A =>: BigDecimal
  implicit def string[A](v: scala.Predef.String): A =>: String
  implicit def boolean[A](v: scala.Boolean): A =>: Boolean
  implicit def byte[A](v: scala.Byte): A =>: Byte
  implicit def `null`[A](v: Null): A =>: Null
  implicit def short[A](v: scala.Short): A =>: Short
  implicit def instant[A](v: java.time.Instant): A =>: java.time.Instant
  implicit def localDate[A](v: java.time.LocalDate): A =>: java.time.LocalDate
  implicit def tuple2[A, B, C](t: (A =>: B, A =>: C)): A =>: (B, C)

  implicit def tuple2Lift[A, B, C](
    t: (B, C)
  )(implicit ev1: B => A =>: B, ev2: C => A =>: C): A =>: (B, C) =
    tuple2((ev1(t._1), ev2(t._2)))

  implicit def tuple2Lift1[A, B, C](t: (A =>: B, C))(implicit ev: C => A =>: C): A =>: (B, C) =
    tuple2((t._1, ev(t._2)))

  implicit def tuple2Lift2[A, B, C](t: (B, A =>: C))(implicit ev1: B => A =>: B): A =>: (B, C) =
    tuple2((ev1(t._1), t._2))

  val setOps: Ops[DataSet]
  val streamOps: Ops[DataStream]
  val stdLib: StandardLibrary

  /**
   * Described a transformation from some Unknown Column type to a concrete Type
   */
  def column[A: Type](str: scala.Predef.String): Unknown =>: A

  /**
   * Loads a [[DataSet]] without type information
   */
  def load(path: String): DataSet[Unknown]

  /**
   * Execute the [[DataStream]] or [[DataSet]]
   */
  def run[A](d: DataStream[A]): IO[Error, Seq[A]]
}
