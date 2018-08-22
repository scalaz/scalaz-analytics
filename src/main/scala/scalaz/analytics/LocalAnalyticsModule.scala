package scalaz.analytics

import scalaz.zio.IO

/**
 * A non distributed implementation of Analytics Module
 */
trait LocalAnalyticsModule extends AnalyticsModule {
  override type DataStream[A] = LocalDataStream
  override type Type[A]       = LocalType[A]
  override type Unknown       = UnknownType
  override type =>:[-A, +B]   = RowFunction
  type UnknownType

  private object LocalNum {

    def apply[A: Type]: Numeric[A] =
      new Numeric[A] {
        override val typeOf             = Type[A]
        override def mult: (A, A) =>: A = RowFunction.Mult(Type[A].reified)
        override def sum: (A, A) =>: A  = RowFunction.Sum(Type[A].reified)
        override def diff: (A, A) =>: A = RowFunction.Diff(Type[A].reified)
        override def mod: (A, A) =>: A  = RowFunction.Mod(Type[A].reified)
      }
  }

  implicit override val unknown: Type[Unknown] = new Type[Unknown] {
    override def reified: Reified = Reified.Unknown
  }
  implicit override val intType: Type[scala.Int]       = LocalType(Reified.Int)
  implicit override val intNumeric: Numeric[scala.Int] = LocalNum[Int]

  implicit override val longType: Type[scala.Long]       = LocalType(Reified.Long)
  implicit override val longNumeric: Numeric[scala.Long] = LocalNum[scala.Long]

  implicit override val floatType: Type[scala.Float]       = LocalType(Reified.Float)
  implicit override val floatNumeric: Numeric[scala.Float] = LocalNum[scala.Float]

  implicit override val doubleType: Type[scala.Double]       = LocalType(Reified.Double)
  implicit override val doubleNumeric: Numeric[scala.Double] = LocalNum[scala.Double]

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
    case object String                        extends Reified
    case object Decimal                       extends Reified
    case object Unknown                       extends Reified
    case class Tuple2(a: Reified, b: Reified) extends Reified
  }

  /**
   * A reified DataStream program
   */
  sealed trait LocalDataStream

  object LocalDataStream {
    case class Empty(rType: Reified)                             extends LocalDataStream
    case class Union(a: LocalDataStream, b: LocalDataStream)     extends LocalDataStream
    case class Intersect(a: LocalDataStream, b: LocalDataStream) extends LocalDataStream
    case class Except(a: LocalDataStream, b: LocalDataStream)    extends LocalDataStream
    case class Distinct(a: LocalDataStream)                      extends LocalDataStream
    case class Map(d: LocalDataStream, f: RowFunction)           extends LocalDataStream
    case class Sort(a: LocalDataStream)                          extends LocalDataStream
    case class DistinctBy(d: LocalDataStream, f: RowFunction)    extends LocalDataStream
  }

  override val setOps: SetOperations = new SetOperations {
    override def union[A](l: LocalDataStream, r: LocalDataStream): LocalDataStream =
      LocalDataStream.Union(l, r)

    override def intersect[A](l: LocalDataStream, r: LocalDataStream): LocalDataStream =
      LocalDataStream.Intersect(l, r)

    override def except[A](l: LocalDataStream, r: LocalDataStream): LocalDataStream =
      LocalDataStream.Except(l, r)

    override def distinct[A](d: LocalDataStream): LocalDataStream =
      LocalDataStream.Distinct(d)

    override def map[A, B](d: LocalDataStream)(f: A =>: B): LocalDataStream =
      LocalDataStream.Map(d, f)

    override def sort[A](d: LocalDataStream): LocalDataStream =
      LocalDataStream.Sort(d)

    override def distinctBy[A, B](d: LocalDataStream)(by: A =>: B): LocalDataStream =
      LocalDataStream.DistinctBy(d, by)
  }

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
    case class FanOut(fst: RowFunction, snd: RowFunction)     extends RowFunction
    case class Split(f: RowFunction, g: RowFunction)          extends RowFunction
    case class Product(fab: RowFunction)                      extends RowFunction
    case class Column(colName: String, rType: Reified)        extends RowFunction

    // constants
    case class IntLiteral(value: Int)            extends RowFunction
    case class LongLiteral(value: Long)          extends RowFunction
    case class FloatLiteral(value: Float)        extends RowFunction
    case class DoubleLiteral(value: Double)      extends RowFunction
    case class DecimalLiteral(value: BigDecimal) extends RowFunction
    case class StringLiteral(value: String)      extends RowFunction
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
  }

  override def empty[A: Type]: LocalDataStream = LocalDataStream.Empty(LocalType.typeOf[A])

  override def int[A](v: scala.Int): A =>: Int                   = RowFunction.IntLiteral(v)
  override def long[A](v: scala.Long): A =>: Long                = RowFunction.LongLiteral(v)
  override def float[A](v: scala.Float): A =>: Float             = RowFunction.FloatLiteral(v)
  override def double[A](v: scala.Double): A =>: Double          = RowFunction.DoubleLiteral(v)
  override def decimal[A](v: scala.BigDecimal): A =>: BigDecimal = RowFunction.DecimalLiteral(v)
  override def string[A](v: scala.Predef.String): A =>: String   = RowFunction.StringLiteral(v)


  // todo this needs more thought
  override def column[A: Type](str: String): Unknown =>: A =
    RowFunction.Column(str, LocalType.typeOf[A])

  def load(path: String): DataStream[Unknown] = ???

  def run[A](d: DataStream[A]): IO[Error, Seq[A]] = ???
}
