import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.functions.randn

object ctrfile {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("bid")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new SQLContext(sc)
    Logger.getRootLogger.setLevel(Level.ERROR)
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    val date=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date1=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date2=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date3=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date4=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date5=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date6=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date7=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date8=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date9=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date10=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date11=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date12=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date13=dateFormat.format(cal.getTime)
    cal.add(Calendar.DATE,-1)
    val date14=dateFormat.format(cal.getTime)

    val rawdata=spark.read.parquet(s"/user/dm/weixiang.jia/feature/ctrprediction/day/$date")
    val rdata=rawdata.drop("request_id","month","week","tp")
    rdata.rdd.map(line=>line.toSeq.mkString(",")).repartition(1).saveAsTextFile(s"/user/dm/huangyukun/ctr/fnn/basedata/$date"+"base")

    val rawdata1=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata2=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date1"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata3=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date2"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata4=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date3"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata5=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date4"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata6=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date5"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata7=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date6"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata8=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date7"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata9=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date8"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata10=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date9"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata11=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date10"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata12=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date11"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata13=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date12"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata14=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date13"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata15=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata/$date14"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val data=rawdata1.union(rawdata2).union(rawdata3).union(rawdata4).union(rawdata5).union(rawdata6).union(rawdata7).union(rawdata8).union(rawdata9).union(rawdata10).union(rawdata11).union(rawdata12).union(rawdata13).union(rawdata14).union(rawdata15)
    val rf=data.withColumn("r",randn).sort("r").drop("r")
    rf.rdd.map(line=>line.toSeq.mkString(",")).repartition(1).saveAsTextFile("/user/dm/huangyukun/ctr/fnn/basedata/traindata")
  }
}