import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

object Assignment extends App{


  val spark = SparkSession.builder()
    .appName("Repartition and Coalesce")
    .master("local[*]")
    .getOrCreate()

  //1.
  val df = spark.read.option("header", true).csv("file:///Users/danishakhlaque/Desktop/retail.csv")
  //2.
  df.printSchema()


  //3.
  val df2 = df.select(df.columns.map {
    case column@"Quantity" =>
      col(column).cast("Int").as(column)
    case column@"Profit" =>
      col(column).cast("Int").as(column)
    case column@"Shipping_Cost" =>
      col(column).cast("Int").as(column)
      case column@"Sales" =>
    col(column).cast("Int").as(column)
    case column@"Discount" =>
      col(column).cast("Int").as(column)
    case column =>
      col(column)
  }: _*).cache()
  //df2.printSchema()

  //println(df2.count())


  //4.
  val df3 = df2.withColumn("NewColumn", col("Quantity") + col("Profit") + col("Shipping_Cost")).show(true)

  //5.
  val df4 = df2.drop("NewColumn")
  df4.show()

  //6.
  df4.explain(true)

  //7
  df2.show(2,false)
  println(df2.count())
  //12
  df2.filter(col("Order_Date").like("2013%")).show()


  df2.groupBy("Ship_Mode").sum("Sales").show()

  df2.groupBy("Ship_Mode", "Category").sum("Sales","Discount").show()

  df2.write.option("header",true)
    .csv("file:///Users/danishakhlaque/Desktop/output.csv")

}
