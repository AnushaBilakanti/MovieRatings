import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
object assign1 {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

//    val conf = new SparkConf().setAppName("Assignment1").setMaster("local")
//    val sc = new SparkContext(conf)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    //var data = sc.textFile("C:\\Anu\\Anu_Files\\After_Coming_USC\\Dec_2017_Work\\SPARK\\DM\\assignment1\\ratings.csv") //Location of the data file
//      .map(line => line.split(","))


//      .map(userRecord => (userRecord(0),
//        userRecord(1), userRecord(2),userRecord(3)))

    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Anu\\Anu_Files\\After_Coming_USC\\Dec_2017_Work\\SPARK\\DM\\assignment1\\ratings.csv")


//    val header=data.first()
//    data=data.filter(row=>row!=header)
    // data.collect().foreach(println)

    //var df=data.toDF()
    //val grouped=data.groupBy(line=>line(1))

    //val res=grouped.map(line=>(line(0),avg(line(1))))
    //println(s"Number of Records in small ratings file ${grouped.take(5).foreach(println)} \n")
    //println(s"Number of Records in small ratings file ${grouped.collect} \n")
    //println(s"Number of Records in small ratings file ${df.show()} \n")






    //println(s"Number of Records in small ratings file ${res.show()}\n")
//    var df2=res.select(res("movieId").cast(IntegerType).as("movieId"),res("avg(rating)").cast(IntegerType).as("rating"))
//    //.agg(avg($"rating"))s
//    val result=df2.orderBy(asc("movieId"))
//
//    //val r=df.select("rating").rdd.map(r => r(0)).collect()
//    println(s"Number of Records in small ratings file ${result.show()}\n")
//    println(s"Number of Records in small ratings file ${data.take(5).foreach(println)} \n")

    //TASK 1
    val res=df.groupBy("movieId").agg(avg(df("rating")))
    var df2=res.select(res("movieId").cast(IntegerType).as("movieId"),res("avg(rating)"))
    val result=df2.orderBy(asc("movieId"))

    //println(s"Number of Records in small ratings file ${result.show()}\n")


    //TASK 2


    val dfr = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Anu\\Anu_Files\\After_Coming_USC\\Dec_2017_Work\\SPARK\\DM\\assignment1\\tags.csv")

    val dfjoin=df.join(dfr,df("movieId")===dfr("movieId") && df("userId")===dfr("userId")).drop(df("movieId"))
    val dfjoin1=dfjoin.drop(df("userId"))

    //println(s"JOIN ${dfjoin1.show()}\n")
    val res1=dfjoin1.groupBy("tag").agg(avg(df("rating")))
   // var df2=res1.select(res("movieId").cast(IntegerType).as("movieId"),res("avg(rating)"))
    val result2=res1.orderBy(desc("tag"))

    println(s"Number of Records in small ratings file ${result2.show()}\n")
  }
}