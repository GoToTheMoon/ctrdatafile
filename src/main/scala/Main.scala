import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val model=args(0)
    val sparkConf = new SparkConf().setAppName("bid")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new SQLContext(sc)
    Logger.getRootLogger.setLevel(Level.ERROR)
    val sqlContext = new SQLContext(sc)

    model match {
      case "pcdata"=>Dataprepare.get_click_data(spark,sc,1)
      case "pctraindata"=> ctrpcfile.get_pc_train_file(spark,sc)
      case "gdt_imei"=>gdt_imei_act.get_file(spark,sc,sqlContext)
    }
  }
}