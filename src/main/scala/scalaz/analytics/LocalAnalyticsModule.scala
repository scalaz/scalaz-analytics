package scalaz.analytics

import scalaz.zio.IO

/**
  * A non distributed implementation of Analytics Module
  */
trait LocalAnalyticsModule extends AnalyticsModule {
  override type DataStream[A] = LocalDataStream
  override type Type[A] = TypeTypeclass[A]
  override type NumericType[A] = NumericTypeTypeclass[A]
  override type Unknown = UnknownType
  type UnknownType
  override type =>:[-A, +B] = RowFunction

  override implicit val unknown: Type[Unknown] = new Type[Unknown] {
    override def typeof: ReifiedType = ReifiedType.Unknown
  }
  override implicit val intNumeric: NumericType[scala.Int] = new NumericType[scala.Int] {
    override def typeof: NumericReifiedType = ReifiedType.Int
  }
  override implicit val longNumeric: NumericType[scala.Long] = new NumericType[scala.Long] {
    override def typeof: NumericReifiedType = ReifiedType.Long
  }
  override implicit val floatNumeric: NumericType[scala.Float] = new NumericType[scala.Float] {
    override def typeof: NumericReifiedType = ReifiedType.Float
  }
  override implicit val doubleNumeric: NumericType[scala.Double] = new NumericType[scala.Double] {
    override def typeof: NumericReifiedType = ReifiedType.Double
  }
  override implicit def tuple2Type[A: Type, B: Type]: Type[(A, B)] = new Type[(A, B)] {
    override def typeof: ReifiedType = ReifiedType.Tuple2(TypeTypeclass.typeof[A], TypeTypeclass.typeof[B])
  }
  override implicit def numericType[A: NumericType]: Type[A] = new Type[A] {
    override def typeof: ReifiedType = NumericTypeTypeclass.typeof[A]
  }


  /**
    * A typeclass that produces a ReifiedType
    */
  sealed trait TypeTypeclass[A] {
    def typeof: ReifiedType
  }
  object TypeTypeclass {
    def typeof[A](implicit ev: TypeTypeclass[A]): ReifiedType = ev.typeof
  }

  /**
    * A typeclass that produces a NumericReifiedType
    */
  sealed trait NumericTypeTypeclass[A] extends TypeTypeclass[A] {
    override def typeof: NumericReifiedType
  }
  object NumericTypeTypeclass {
    def typeof[A](implicit ev: NumericTypeTypeclass[A]): NumericReifiedType = ev.typeof
  }


  /**
    * The set of reified types.
    * These represent all the Types that scalaz-analytics works with
    */
  sealed trait ReifiedType
  object ReifiedType {
    case object Int extends NumericReifiedType
    case object Long extends NumericReifiedType
    case object Float extends NumericReifiedType
    case object Double extends NumericReifiedType
    case object String extends ReifiedType
    case object Decimal extends NumericReifiedType
    case object Unknown extends ReifiedType
    case class Tuple2(a: ReifiedType, b: ReifiedType) extends ReifiedType
  }
  
  /**
    * A Marker for types that are numeric
    */
  trait NumericReifiedType extends ReifiedType

  /**
    * A reified DataStream program
    */
  sealed trait LocalDataStream
  object LocalDataStream {
    case class Empty(rType: ReifiedType) extends LocalDataStream
    case class Union(a: LocalDataStream, b: LocalDataStream) extends LocalDataStream
    case class Intersect(a: LocalDataStream, b: LocalDataStream) extends LocalDataStream
    case class Except(a: LocalDataStream, b: LocalDataStream) extends LocalDataStream
    case class Distinct(a: LocalDataStream) extends LocalDataStream
    case class Map[A, B](d: LocalDataStream, f: A =>: B) extends LocalDataStream
    case class Sort(a: LocalDataStream) extends LocalDataStream
    case class DistinctBy[A, B](d: LocalDataStream, f: A =>: B) extends LocalDataStream
  }

  override val setOps: SetOperations = new SetOperations {
    override def empty[A: Type]: LocalDataStream = LocalDataStream.Empty(TypeTypeclass.typeof[A])

    override def union[A: Type](l: LocalDataStream, r: LocalDataStream): LocalDataStream = LocalDataStream.Union(l, r)

    override def intersect[A: Type](l: LocalDataStream, r: LocalDataStream): LocalDataStream = LocalDataStream.Intersect(l, r)

    override def except[A: Type](l: LocalDataStream, r: LocalDataStream): LocalDataStream = LocalDataStream.Except(l, r)

    override def distinct[A: Type](d: LocalDataStream): LocalDataStream = LocalDataStream.Distinct(d)

    override def map[A: Type, B: Type](d: LocalDataStream)(f: A =>: B): LocalDataStream = LocalDataStream.Map[A, B](d, f)

    override def sort[A: Type](d: LocalDataStream): LocalDataStream = LocalDataStream.Sort(d)

    override def distinctBy[A: Type, B: Type](d: LocalDataStream)(by: A =>: B): LocalDataStream = LocalDataStream.DistinctBy[A, B](d, by)
  }


  /**
    * An implementation of the arrow (=>:) in AnalyticsModule
    * This allows us to reify all the operations.
    */
  sealed trait RowFunction
  object RowFunction {
    case class Id(reifiedType: ReifiedType) extends RowFunction
    case class Compose(left: RowFunction, right: RowFunction) extends RowFunction
    case class Mult(typ: NumericReifiedType) extends RowFunction
    case class Sum(typ: NumericReifiedType) extends RowFunction
    case class Diff(typ: NumericReifiedType) extends RowFunction
    case class Mod(typ: NumericReifiedType) extends RowFunction
    case class FanOut(fst: RowFunction, snd: RowFunction) extends RowFunction
    case class Split(f: RowFunction, g: RowFunction) extends RowFunction
    case class Product(fab: RowFunction) extends RowFunction
    case class Column(colName: String, rType: ReifiedType) extends RowFunction

    // constants
    case class IntLiteral(value: Int) extends RowFunction
    case class LongLiteral(value: Long) extends RowFunction
    case class FloatLiteral(value: Float) extends RowFunction
    case class DoubleLiteral(value: Double) extends RowFunction
    case class DecimalLiteral(value: BigDecimal) extends RowFunction
    case class StringLiteral(value: String) extends RowFunction
  }

  override val stdLib: StandardLibrary = new StandardLibrary {
    override def id[A: Type]: A =>: A = RowFunction.Id(TypeTypeclass.typeof[A])

    override def compose[A: Type, B: Type, C: Type](f: B =>: C, g: A =>: B): A =>: C = RowFunction.Compose(f, g)

    override def andThen[A: Type, B: Type, C: Type](f: A =>: B, g: B =>: C): A =>: C = RowFunction.Compose(g, f)

    override def mult[A: NumericType]: (A, A) =>: A = RowFunction.Mult(NumericTypeTypeclass.typeof[A])

    override def sum[A: NumericType]: (A, A) =>: A = RowFunction.Sum(NumericTypeTypeclass.typeof[A])

    override def diff[A: NumericType]: (A, A) =>: A = RowFunction.Diff(NumericTypeTypeclass.typeof[A])

    override def mod[A: NumericType]: (A, A) =>: A = RowFunction.Mod(NumericTypeTypeclass.typeof[A])

    override def fanOut[A: Type, B: Type, C: Type](fst: A =>: B, snd: A =>: C): A =>: (B, C) = RowFunction.FanOut(fst, snd)

    override def split[A: Type, B: Type, C: Type, D: Type](f: A =>: B, g: C =>: D): (A, C) =>: (B, D) = RowFunction.Split(f, g)

    override def product[A: Type, B: Type](fab: A =>: B): (A, A) =>: (B, B) = RowFunction.Product(fab)

    override def int[A: Type](v: scala.Int): A =>: Int = RowFunction.IntLiteral(v)
    override def long[A: Type](v: scala.Long): A =>: Long = RowFunction.LongLiteral(v)
    override def float[A: Type](v: scala.Float): A =>: Float = RowFunction.FloatLiteral(v)
    override def double[A: Type](v: scala.Double): A =>: Double = RowFunction.DoubleLiteral(v)
    override def decimal[A: Type](v: scala.BigDecimal): A =>: BigDecimal = RowFunction.DecimalLiteral(v)
    override def string[A: Type](v: scala.Predef.String): A =>: String = RowFunction.StringLiteral(v)

    // todo this needs more thought
    override def column[A: Type](str: String): Unknown =>: A = RowFunction.Column(str, TypeTypeclass.typeof[A])
  }

  def load(path: String): DataStream[Unknown] = ???

  def run[A: Type](d: DataStream[A]): IO[Error, Seq[A]] = ???
}

