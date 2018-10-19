import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.randn

object ctrpcfile {
  def get_pc_train_file(spark:SparkSession,sc:SparkContext): Unit ={

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

    val rawdata=spark.read.parquet(s"/user/dm/huangyukun/ctr/day_basedata_pc/$date")
    val rdata=rawdata.drop("request_id","month","week","tp")
    rdata.rdd.map(line=>line.toSeq.mkString(",")).repartition(1).saveAsTextFile(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date"+"base")

    val rawdata1=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata2=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date1"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata3=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date2"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata4=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date3"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata5=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date4"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata6=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date5"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata7=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date6"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata8=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date7"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata9=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date8"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata10=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date9"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata11=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date10"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata12=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date11"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata13=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date12"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata14=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date13"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)
    val rawdata15=spark.read.format("csv").option("header","false").csv(s"/user/dm/huangyukun/ctr/fnn/basedata_pc/$date14"+"base").withColumn("r",randn).sort("r").drop("r").limit(1000000)

    val data=rawdata1.union(rawdata2).union(rawdata3).union(rawdata4).union(rawdata5).union(rawdata6).union(rawdata7).union(rawdata8).union(rawdata9).union(rawdata10).union(rawdata11).union(rawdata12).union(rawdata13).union(rawdata14).union(rawdata15)
    val rf=data.withColumn("r",randn).sort("r").drop("r")
    rf.rdd.map(line=>line.toSeq.mkString(",")).repartition(1).saveAsTextFile("/user/dm/huangyukun/ctr/fnn/basedata_pc/traindata")
  }
}