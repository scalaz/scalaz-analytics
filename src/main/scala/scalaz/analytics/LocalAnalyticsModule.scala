package scalaz.analytics

import scalaz.zio.IO

/**
  * A non distributed implementation of Analytics Module
  */
trait LocalAnalyticsModule extends AnalyticsModule {
  override type DataStream[A] = LocalDataStream
  override type Type[A] = TypeTypeclass[A]
  override type Unknown = UnknownType
  type UnknownType
  override type =>:[-A, +B] = RowFunction

  private object Nums {
    def apply[A: Type]: Num[A] =
      new Num[A] {
        override val typeOf = Type[A]
        override def mult: (A, A) =>: A = RowFunction.Mult(Type[A].reified)
        override def sum: (A, A) =>: A = RowFunction.Sum(Type[A].reified)
        override def diff: (A, A) =>: A = RowFunction.Diff(Type[A].reified)
        override def mod: (A, A) =>: A = RowFunction.Mod(Type[A].reified)
      }
  }

  override implicit val unknown: Type[Unknown] = new Type[Unknown] {
    override def reified: ReifiedType = ReifiedType.Unknown
  }
  override implicit val intType: Type[scala.Int] = ReifiedType(ReifiedType.Int)
  override implicit val intNumeric: Num[scala.Int] = Nums[Int]

  override implicit val longType: Type[scala.Long] = ReifiedType(ReifiedType.Long)
  override implicit val longNumeric: Num[scala.Long] = Nums[scala.Long]

  override implicit val floatType: Type[scala.Float] = ReifiedType(ReifiedType.Float)
  override implicit val floatNumeric: Num[scala.Float] =  Nums[scala.Float]

  override implicit val doubleType: Type[scala.Double] = ReifiedType(ReifiedType.Double)
  override implicit val doubleNumeric: Num[scala.Double] =  Nums[scala.Double]

  override implicit def tuple2Type[A: Type, B: Type]: Type[(A, B)] = new Type[(A, B)] {
    override def reified: ReifiedType = ReifiedType.Tuple2(TypeTypeclass.typeof[A], TypeTypeclass.typeof[B])
  }


  /**
    * A typeclass that produces a ReifiedType
    */
  sealed trait TypeTypeclass[A] {
    def reified: ReifiedType
  }
  object TypeTypeclass {
    def typeof[A](implicit ev: TypeTypeclass[A]): ReifiedType = ev.reified
  }

  /**
    * The set of reified types.
    * These represent all the Types that scalaz-analytics works with
    */
  sealed trait ReifiedType
  object ReifiedType {
    private[LocalAnalyticsModule] def apply[A](r: ReifiedType): Type[A] =
      new Type[A] {
        override def reified: ReifiedType = r
      }

    case object Int extends ReifiedType
    case object Long extends ReifiedType
    case object Float extends ReifiedType
    case object Double extends ReifiedType
    case object String extends ReifiedType
    case object Decimal extends ReifiedType
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
    case class Mult[A](typ: ReifiedType) extends RowFunction
    case class Sum(typ: ReifiedType) extends RowFunction
    case class Diff(typ: ReifiedType) extends RowFunction
    case class Mod[A](typ: ReifiedType) extends RowFunction
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

    override def compose[A, B, C](f: B =>: C, g: A =>: B): A =>: C = RowFunction.Compose(f, g)

    override def andThen[A, B, C](f: A =>: B, g: B =>: C): A =>: C = RowFunction.Compose(g, f)

    override def fanOut[A, B, C](fst: A =>: B, snd: A =>: C): A =>: (B, C) = RowFunction.FanOut(fst, snd)

    override def split[A, B, C, D](f: A =>: B, g: C =>: D): (A, C) =>: (B, D) = RowFunction.Split(f, g)

    override def product[A, B](fab: A =>: B): (A, A) =>: (B, B) = RowFunction.Product(fab)

    override def int[A](v: scala.Int): A =>: Int = RowFunction.IntLiteral(v)
    override def long[A](v: scala.Long): A =>: Long = RowFunction.LongLiteral(v)
    override def float[A](v: scala.Float): A =>: Float = RowFunction.FloatLiteral(v)
    override def double[A](v: scala.Double): A =>: Double = RowFunction.DoubleLiteral(v)
    override def decimal[A](v: scala.BigDecimal): A =>: BigDecimal = RowFunction.DecimalLiteral(v)
    override def string[A](v: scala.Predef.String): A =>: String = RowFunction.StringLiteral(v)

    // todo this needs more thought
    override def column[A: Type](str: String): Unknown =>: A = RowFunction.Column(str, TypeTypeclass.typeof[A])
  }

  def load(path: String): DataStream[Unknown] = ???

  def run[A: Type](d: DataStream[A]): IO[Error, Seq[A]] = ???
}

