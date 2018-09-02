package scalaz.analytics

import scalaz.zio.IO

import scala.language.implicitConversions

/**
 * An analytics module defined the abstract components required to build a reified program description.
 */
trait AnalyticsModule {

  /**
   * An abstract Dataset
   */
  type Dataset[A]

  /**
   * An abstract DataStream.
   */
  type DataStream[A]

  /**
   * A window is a way o specify a view on a DataStream/Dataset
   */
  sealed trait Window

  object Window {
    case class FixedTimeWindow()   extends Window
    case class SlidingTimeWindow() extends Window
    case class SessionWindow()     extends Window
    // For internal use only
    // todo would you ever want a GlobalWindow on a DataStream? Probably not?
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
  implicit val timestampType: Type[java.time.LocalDateTime]
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
   * All operations that don't require a bounded input
   */
  trait Ops[F[_]] {
    // Unbounded
    def map[A, B](ds: F[A])(f: A =>: B): F[B]
    def filter[A, B](ds: F[A])(f: A =>: Boolean): F[A]
    // todo add more

    // Bounded
    def fold[A, B](ds: F[A])(window: Window)(initial: A =>: B)(f: (B, A) =>: B): F[B] // todo is `initial` and `f` correct?
    def distinct[A](ds: F[A])(window: Window): F[A]
    // todo add more

    // todo prototype how joins/unions etc will work
  }

  /**
   * The standard library of scalaz-analytics.
   */
  trait StandardLibrary {
    def id[A: Type]: A =>: A
    def compose[A, B, C](f: B =>: C, g: A =>: B): A =>: C
    def andThen[A, B, C](f: A =>: B, g: B =>: C): A =>: C
    def fanOut[A, B, C](fst: A =>: B, snd: A =>: C): A =>: (B, C)
    def split[A, B, C, D](f: A =>: B, g: C =>: D): (A, C) =>: (B, D)
    def product[A, B](fab: A =>: B): (A, A) =>: (B, B)
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
   * A DSL for building the Dataset data structure
   */
  implicit class DatasetSyntax[A](ds: Dataset[A])(implicit A: Type[A]) {

    def map[B: Type](f: (A =>: A) => (A =>: B)): Dataset[B] =
      setOps.map(ds)(f(stdLib.id))

    def filter(f: (A =>: A) => (A =>: Boolean)): Dataset[A] =
      setOps.filter(ds)(f(stdLib.id))

    def fold[B: Type](init: A =>: B)(f: (B, A) =>: B): Dataset[B] =
      setOps.fold(ds)(Window.GlobalWindow())(init)(f)

    def distinct: Dataset[A] =
      setOps.distinct(ds)(Window.GlobalWindow())
  }

  /**
   * A DSL for building the DataStream data structure
   */
  implicit class DataStreamSyntax[A](ds: DataStream[A])(implicit A: Type[A]) {

    def map[B: Type](f: (A =>: A) => (A =>: B)): DataStream[B] =
      streamOps.map(ds)(f(stdLib.id))

    def filter(f: (A =>: A) => (A =>: Boolean)): DataStream[A] =
      streamOps.filter(ds)(f(stdLib.id))

    def fold[B: Type](window: Window)(init: A =>: B)(f: (B, A) =>: B): DataStream[B] =
      streamOps.fold(ds)(window)(init)(f)

    def distinct(window: Window): DataStream[A] =
      streamOps.distinct(ds)(window)
  }

  /**
   * Create an empty Dataset of type A
   */
  def empty[A: Type]: Dataset[A]

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
  implicit def timestamp[A](v: java.time.LocalDateTime): A =>: java.sql.Timestamp
  implicit def date[A](v: java.time.LocalDate): A =>: java.sql.Date

  val setOps: Ops[Dataset]
  val streamOps: Ops[DataStream]
  val stdLib: StandardLibrary

  /**
   * Described a transformation from some Unknown Column type to a concrete Type
   */
  def column[A: Type](str: scala.Predef.String): Unknown =>: A

  /**
   * Loads a [[Dataset]] without type information
   */
  def load(path: String): Dataset[Unknown]

  /**
   * Execute the [[DataStream]] or [[Dataset]]
   */
  def run[A](d: DataStream[A]): IO[Error, Seq[A]]
}
