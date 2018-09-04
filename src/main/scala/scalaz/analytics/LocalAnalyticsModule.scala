package scalaz.analytics

import scalaz.zio.IO
import scala.language.implicitConversions
import java.time.{ Instant, LocalDate }

/**
 * A non distributed implementation of Analytics Module
 */
trait LocalAnalyticsModule extends AnalyticsModule {
  override type DataSet[A]    = LocalDataStream
  override type DataStream[A] = LocalDataStream
  override type Type[A]       = LocalType[A]
  override type Unknown       = UnknownType
  override type =>:[-A, +B]   = RowFunction
  type UnknownType

  private object LocalNumeric {

    def apply[A: Type]: Numeric[A] =
      new Numeric[A] {
        override val typeOf: Type[A]                 = Type[A]
        override def mult: (A, A) =>: A              = RowFunction.Mult(Type[A].reified)
        override def sum: (A, A) =>: A               = RowFunction.Sum(Type[A].reified)
        override def diff: (A, A) =>: A              = RowFunction.Diff(Type[A].reified)
        override def mod: (A, A) =>: A               = RowFunction.Mod(Type[A].reified)
        override def greaterThan: (A, A) =>: Boolean = RowFunction.GreaterThan(Type[A].reified)
      }
  }

  implicit override val unknown: Type[Unknown] = new Type[Unknown] {
    override def reified: Reified = Reified.Unknown
  }
  implicit override val intType: Type[scala.Int]       = LocalType(Reified.Int)
  implicit override val intNumeric: Numeric[scala.Int] = LocalNumeric[Int]

  implicit override val longType: Type[scala.Long]       = LocalType(Reified.Long)
  implicit override val longNumeric: Numeric[scala.Long] = LocalNumeric[scala.Long]

  implicit override val floatType: Type[scala.Float]       = LocalType(Reified.Float)
  implicit override val floatNumeric: Numeric[scala.Float] = LocalNumeric[scala.Float]

  implicit override val doubleType: Type[scala.Double]       = LocalType(Reified.Double)
  implicit override val doubleNumeric: Numeric[scala.Double] = LocalNumeric[scala.Double]

  implicit override val decimalType: LocalType[scala.math.BigDecimal] = LocalType(
    Reified.BigDecimal
  )
  implicit override val decimalNumeric: Numeric[scala.math.BigDecimal] =
    LocalNumeric[scala.BigDecimal]

  implicit override val stringType: Type[scala.Predef.String] = LocalType(Reified.String)
  implicit override val booleanType: Type[scala.Boolean]      = LocalType(Reified.Boolean)
  implicit override val byteType: Type[scala.Byte]            = LocalType(Reified.Byte)
  implicit override val nullType: Type[scala.Null]            = LocalType(Reified.Null)
  implicit override val shortType: Type[scala.Short]          = LocalType(Reified.Short)
  implicit override val instantType: Type[Instant]            = LocalType(Reified.Instant)
  implicit override val dateType: Type[LocalDate]             = LocalType(Reified.LocalDate)

  implicit override def tuple2Type[A: Type, B: Type]: Type[(A, B)] = new Type[(A, B)] {
    override def reified: Reified = Reified.Tuple2(LocalType.typeOf[A], LocalType.typeOf[B])
  }

  /**
   * A typeclass that produces a Reified
   */
  sealed trait LocalType[A] {
    def reified: Reified
  }

  object LocalType {
    def typeOf[A](implicit ev: LocalType[A]): Reified = ev.reified

    private[LocalAnalyticsModule] def apply[A](r: Reified): Type[A] =
      new Type[A] {
        override def reified: Reified = r
      }
  }

  /**
   * The set of reified types.
   * These represent all the Types that scalaz-analytics works with
   */
  sealed trait Reified

  object Reified {
    case object Int                           extends Reified
    case object Long                          extends Reified
    case object Float                         extends Reified
    case object Double                        extends Reified
    case object BigDecimal                    extends Reified
    case object String                        extends Reified
    case object Decimal                       extends Reified
    case object Boolean                       extends Reified
    case object Byte                          extends Reified
    case object Null                          extends Reified
    case object Short                         extends Reified
    case object Instant                       extends Reified
    case object LocalDate                     extends Reified
    case object Unknown                       extends Reified
    case class Tuple2(a: Reified, b: Reified) extends Reified
  }

  /**
   * A reified DataStream program
   */
  sealed trait LocalDataStream

  object LocalDataStream {
    case class Empty(rType: Reified) extends LocalDataStream

    case class Map(d: LocalDataStream, f: RowFunction)    extends LocalDataStream
    case class Filter(d: LocalDataStream, f: RowFunction) extends LocalDataStream

    case class Fold(d: LocalDataStream, initial: RowFunction, f: RowFunction, window: Window)
        extends LocalDataStream
    case class Distinct(d: LocalDataStream, window: Window) extends LocalDataStream
  }

  private val ops: Ops[DataSet] = new Ops[DataSet] {
    override def map[A, B](ds: LocalDataStream)(f: A =>: B): LocalDataStream =
      LocalDataStream.Map(ds, f)
    override def filter[A](ds: LocalDataStream)(f: A =>: Boolean): LocalDataStream =
      LocalDataStream.Filter(ds, f)

    override def fold[A, B](
      ds: LocalDataStream
    )(window: Window)(initial: A =>: B)(f: (B, A) =>: B): LocalDataStream =
      LocalDataStream.Fold(ds, initial, f, window)
    override def distinct[A](ds: LocalDataStream)(window: Window): LocalDataStream =
      LocalDataStream.Distinct(ds, window)
  }

  override val setOps: Ops[DataSet]       = ops
  override val streamOps: Ops[DataStream] = ops

  /**
   * An implementation of the arrow (=>:) in AnalyticsModule
   * This allows us to reify all the operations.
   */
  sealed trait RowFunction

  object RowFunction {
    case class Id(reifiedType: Reified)                       extends RowFunction
    case class Compose(left: RowFunction, right: RowFunction) extends RowFunction
    case class Mult(typ: Reified)                             extends RowFunction
    case class Sum(typ: Reified)                              extends RowFunction
    case class Diff(typ: Reified)                             extends RowFunction
    case class Mod(typ: Reified)                              extends RowFunction
    case class GreaterThan(typ: Reified)                      extends RowFunction
    case class FanOut(fst: RowFunction, snd: RowFunction)     extends RowFunction
    case class Split(f: RowFunction, g: RowFunction)          extends RowFunction
    case class Product(fab: RowFunction)                      extends RowFunction
    case class Column(colName: String, rType: Reified)        extends RowFunction
    case class ExtractNth(reified: Reified, n: Int)           extends RowFunction

    // constants
    case class IntLiteral(value: Int)             extends RowFunction
    case class LongLiteral(value: Long)           extends RowFunction
    case class FloatLiteral(value: Float)         extends RowFunction
    case class DoubleLiteral(value: Double)       extends RowFunction
    case class DecimalLiteral(value: BigDecimal)  extends RowFunction
    case class StringLiteral(value: String)       extends RowFunction
    case class BooleanLiteral(value: Boolean)     extends RowFunction
    case class ByteLiteral(value: Byte)           extends RowFunction
    case object NullLiteral                       extends RowFunction
    case class ShortLiteral(value: Short)         extends RowFunction
    case class InstantLiteral(value: Instant)     extends RowFunction
    case class LocalDateLiteral(value: LocalDate) extends RowFunction
  }

  override val stdLib: StandardLibrary = new StandardLibrary {
    override def id[A: Type]: A =>: A = RowFunction.Id(LocalType.typeOf[A])

    override def compose[A, B, C](f: B =>: C, g: A =>: B): A =>: C = RowFunction.Compose(f, g)

    override def andThen[A, B, C](f: A =>: B, g: B =>: C): A =>: C = RowFunction.Compose(g, f)

    override def fanOut[A, B, C](fst: A =>: B, snd: A =>: C): A =>: (B, C) =
      RowFunction.FanOut(fst, snd)

    override def split[A, B, C, D](f: A =>: B, g: C =>: D): (A, C) =>: (B, D) =
      RowFunction.Split(f, g)

    override def product[A, B](fab: A =>: B): (A, A) =>: (B, B) = RowFunction.Product(fab)

    override def fst[A: Type, B]: (A, B) =>: A = RowFunction.ExtractNth(Type[A].reified, 0)

    override def snd[A, B: Type]: (A, B) =>: B = RowFunction.ExtractNth(Type[B].reified, 1)
  }

  override def empty[A: Type]: LocalDataStream = LocalDataStream.Empty(LocalType.typeOf[A])

  override def emptyStream[A: Type]: LocalDataStream = LocalDataStream.Empty(LocalType.typeOf[A])

  implicit override def int[A](v: scala.Int): A =>: Int          = RowFunction.IntLiteral(v)
  implicit override def long[A](v: scala.Long): A =>: Long       = RowFunction.LongLiteral(v)
  implicit override def float[A](v: scala.Float): A =>: Float    = RowFunction.FloatLiteral(v)
  implicit override def double[A](v: scala.Double): A =>: Double = RowFunction.DoubleLiteral(v)
  implicit override def decimal[A](v: scala.BigDecimal): A =>: BigDecimal =
    RowFunction.DecimalLiteral(v)
  implicit override def string[A](v: scala.Predef.String): A =>: String =
    RowFunction.StringLiteral(v)
  implicit override def boolean[A](v: scala.Boolean): A =>: Boolean = RowFunction.BooleanLiteral(v)
  implicit override def byte[A](v: scala.Byte): A =>: Byte          = RowFunction.ByteLiteral(v)
  implicit override def `null`[A](v: scala.Null): A =>: Null        = RowFunction.NullLiteral
  implicit override def short[A](v: scala.Short): A =>: Short       = RowFunction.ShortLiteral(v)
  implicit override def instant[A](v: Instant): A =>: Instant       = RowFunction.InstantLiteral(v)
  implicit override def localDate[A](v: LocalDate): A =>: LocalDate =
    RowFunction.LocalDateLiteral(v)
  implicit override def tuple2[A, B, C](t: (A =>: B, A =>: C)): A =>: (B, C) =
    RowFunction.FanOut(t._1, t._2)

  // todo this needs more thought
  override def column[A: Type](str: String): Unknown =>: A =
    RowFunction.Column(str, LocalType.typeOf[A])

  def load(path: String): DataStream[Unknown] = ???

  def run[A](d: DataStream[A]): IO[Error, Seq[A]] = ???
}
