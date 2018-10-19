import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.collection.mutable

object gdt_imei_act extends Serializable{

  val schema = StructType(
    Seq(
      StructField("logdatetime",StringType,nullable = true),
      StructField("reqid", StringType,nullable = true),
      StructField("appid", StringType,nullable = true) ,
      StructField("codeid", StringType,nullable = true),
      StructField("src", StringType,nullable = true),
      StructField("winprice", StringType,nullable = true),
      StructField("reqtime", StringType,nullable = true),
      StructField("ip", StringType,nullable = true),
      StructField("ua", StringType,nullable = true),
      StructField("ostype", StringType,nullable = true),
      StructField("dt", StringType,nullable = true),
      StructField("imei", StringType,nullable = true),
      StructField("idfa", StringType,nullable = true),
      StructField("androidid", StringType,nullable = true),
      StructField("ot", StringType,nullable = true),
      StructField("osver", StringType,nullable = true),
      StructField("vendor", StringType,nullable = true),
      StructField("model", StringType,nullable = true),
      StructField("appver", StringType,nullable = true),
      StructField("adid", StringType,nullable = true),
      StructField("mac", StringType,nullable = true),
      StructField("creativetype", StringType,nullable = true),
      StructField("adw", StringType,nullable = true),
      StructField("adh", StringType,nullable = true),
      StructField("title", StringType,nullable = true),
      StructField("desc", StringType,nullable = true),
      StructField("sw", StringType,nullable = true),
      StructField("sh", StringType,nullable = true),
      StructField("ct", StringType,nullable = true),
      StructField("clcurl", StringType,nullable = true)
    )
  )

  def gdtdf(rDD: RDD[String],spark: SparkSession):DataFrame={
    val gdt=rDD.filter(x=>x.split("\\|\\|").length==31).map{x=>
      val xs=x.split("\\|\\|")
      val logdatetime=if (xs(0).split("=").length==2) xs(0).split("=")(1) else null
      val reqid=if (xs(1).split("=").length==2) xs(1).split("=")(1) else null
      val appid=if (xs(2).split("=").length==2) xs(2).split("=")(1) else null
      val codeid=if (xs(3).split("=").length==2) xs(3).split("=")(1) else null
      val src=if (xs(4).split("=").length==2) xs(4).split("=")(1) else null
      val winprice=if (xs(5).split("=").length==2) xs(5).split("=")(1) else null
      val reqtime=if (xs(6).split("=").length==2) xs(6).split("=")(1) else null
      val ip=if (xs(7).split("=").length==2) xs(7).split("=")(1) else null
      val ua=if (xs(8).split("=").length==2) xs(8).split("=")(1) else null
      val ostype=if (xs(9).split("=").length==2) xs(9).split("=")(1) else null
      val dt=if (xs(10).split("=").length==2) xs(10).split("=")(1) else null
      val imei=if (xs(11).split("=").length==2) xs(11).split("=")(1) else null
      val idfa=if (xs(12).split("=").length==2) xs(12).split("=")(1) else null
      val androidid=if (xs(13).split("=").length==2) xs(13).split("=")(1) else null
      val ot=if (xs(14).split("=").length==2) xs(14).split("=")(1) else null
      val osver=if (xs(15).split("=").length==2) xs(15).split("=")(1) else null
      val vendor=if (xs(16).split("=").length==2) xs(16).split("=")(1) else null
      val model=if (xs(17).split("=").length==2) xs(17).split("=")(1) else null
      val appver=if (xs(18).split("=").length==2) xs(18).split("=")(1) else null
      val adid=if (xs(19).split("=").length==2) xs(19).split("=")(1) else null
      val mac=if (xs(20).split("=").length==2) xs(20).split("=")(1) else null
      val creativetype=if (xs(21).split("=").length==2) xs(21).split("=")(1) else null
      val adw=if (xs(22).split("=").length==2) xs(22).split("=")(1) else null
      val adh=if (xs(23).split("=").length==2) xs(23).split("=")(1) else null
      val title=if (xs(24).split("=").length==2) xs(24).split("=")(1) else null
      val desc=if (xs(25).split("=").length==2) xs(25).split("=")(1) else null
      val sw=if (xs(26).split("=").length==2) xs(26).split("=")(1) else null
      val sh=if (xs(27).split("=").length==2) xs(27).split("=")(1) else null
      val ct=if (xs(28).split("=").length==2) xs(28).split("=")(1) else null
      val clcurl=if (xs(29).split("=").length==2) xs(29).split("=")(1) else null

      Row(logdatetime,reqid,appid,codeid,src,winprice,reqtime,ip,ua,ostype,dt,imei,idfa,androidid,ot,osver,
        vendor,model,appver,adid,mac,creativetype,adw,adh,title,desc,sw,sh,ct,clcurl)
    }
    spark.createDataFrame(gdt,schema)
  }

  val schema1 = StructType(
    Seq(
      StructField("adid",StringType,nullable = true),
      StructField("id",StringType,nullable = true),
      StructField("adtype",StringType,nullable = true),
      StructField("width",StringType,nullable = true),
      StructField("height",StringType,nullable = true),
      StructField("channel",StringType,nullable = true),
      StructField("imgsrc",StringType,nullable = true),
      StructField("imgsrcmd5",StringType,nullable = true),
      StructField("title",StringType,nullable = true),
      StructField("desc",StringType,nullable = true),
      StructField("createtime",StringType,nullable = true),
      StructField("updatetime",StringType,nullable = true),
      StructField("logdatetime",StringType,nullable = true),
      StructField("reqid", StringType,nullable = true),
      StructField("appid", StringType,nullable = true) ,
      StructField("codeid", StringType,nullable = true),
      StructField("src", StringType,nullable = true),
      StructField("winprice", StringType,nullable = true),
      StructField("reqtime", StringType,nullable = true),
      StructField("ip", StringType,nullable = true),
      StructField("ua", StringType,nullable = true),
      StructField("ostype", StringType,nullable = true),
      StructField("dt", StringType,nullable = true),
      StructField("imei", StringType,nullable = true),
      StructField("idfa", StringType,nullable = true),
      StructField("androidid", StringType,nullable = true),
      StructField("ot", StringType,nullable = true),
      StructField("osver", StringType,nullable = true),
      StructField("vendor", StringType,nullable = true),
      StructField("model", StringType,nullable = true),
      StructField("appver", StringType,nullable = true),
      StructField("mac", StringType,nullable = true),
      StructField("creativetype", StringType,nullable = true),
      StructField("adw", StringType,nullable = true),
      StructField("adh", StringType,nullable = true),
      StructField("sw", StringType,nullable = true),
      StructField("sh", StringType,nullable = true),
      StructField("ct", StringType,nullable = true),
      StructField("clcurl", StringType,nullable = true),
      StructField("label", StringType,nullable = true)
    )
  )

  def gdt_read_train_data(rDD: RDD[String],spark: SparkSession):DataFrame={
    val gdt=rDD.filter(x=>x.split("\\|\\|").length==40).map{x=>
      val xs=x.split("\\|\\|")
      val adid= xs(0)
      val id= xs(1)
      val adtype= xs(2)
      val width=xs(3)
      val height=xs(4)
      val channel=xs(5)
      val imgsrc=xs(6)
      val imgsrcmd5=xs(7)
      val title=xs(8)
      val desc=xs(9)
      val createtime=xs(10)
      val updatetime=xs(11)
      val logdatetime=xs(12)
      val reqid=xs(13)
      val appid=xs(14)
      val codeid=xs(15)
      val src=xs(16)
      val winprice=xs(17)
      val reqtime=xs(18)
      val ip=xs(19)
      val ua=xs(20)
      val ostype=xs(21)
      val dt=xs(22)
      val imei=xs(23)
      val idfa=xs(24)
      val androidid=xs(25)
      val ot=xs(26)
      val osver=xs(27)
      val vendor=xs(28)
      val model=xs(29)
      val appver=xs(30)
      val mac=xs(31)
      val creativetype=xs(32)
      val adw=xs(33)
      val adh=xs(34)
      val sw=xs(35)
      val sh=xs(36)
      val ct=xs(37)
      val clcurl=xs(38)
      val label=xs(39)

      Row(adid,id,adtype,width,height,channel,imgsrc,imgsrcmd5,title,desc,createtime,updatetime,logdatetime,reqid,appid,codeid,src,winprice,reqtime,ip,ua,ostype,dt,imei,idfa,androidid,ot,osver,
        vendor,model,appver,mac,creativetype,adw,adh,sw,sh,ct,clcurl,label)
    }

    spark.createDataFrame(gdt,schema1)
  }



  def get_file(spark: SparkSession,  sc: SparkContext,sqlContext:SQLContext): Unit ={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance

    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val yesterday=format.format(time)

    cal.add(Calendar.DATE, -1)
    val time1 = cal.getTime
    val thedaybeforeyesterday=format.format(time1)

    cal.add(Calendar.DATE, -1)
    val time2 = cal.getTime
    val threedaysago=format.format(time2)

    val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://l-db14-1.prod.qd1.corp.agrant.cn:3306/ZCG_AD_DB", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "Ad", "user" -> "dev", "password" -> "vKITJVGT7dianJMXDNERlcK2zYEbVkutEShK69SFDxTlIJF3SjLlHCbhZcfw")).load()
    val gdtdata =sc.textFile(s"/user/ssp/log/adshow*$yesterday*lzo")
    val gdtdataclick =sc.textFile(s"/user/ssp/log/adclick*$yesterday*lzo")
    val gdtDF=gdtdf(gdtdata,spark)
    val gdtDFclick=gdtdf(gdtdataclick,spark)
    val gdt5=gdtDF.filter("src==5 and adid is not null").drop("title","desc").withColumn("label",lit(0))
    val gdt5click=gdtDFclick.filter("src==5 and adid is not null").drop("title","desc").withColumn("label",lit(1))
    val gdt5all=gdt5.union(gdt5click)
    jdbcDF.join(gdt5all,Seq("adid")).withColumn("r",randn()).sort("r").drop("r").rdd.map(line=>line.toSeq.mkString("||")).repartition(1).saveAsTextFile(s"gdt_ad_log/$yesterday")

    val gdt_adct=sc.textFile("/user/dm/huangyukun/gdt/ctr/gdt_adc.txt")

    val gdt_map= new mutable.HashMap[String,Array[String]]()
    gdt_adct.take(gdt_adct.count.toInt).map{x=>
      val xs=x.split("：")
      gdt_map.put(xs(0),xs(1).split("、"))
    }

    val gdt_train_log1=sc.textFile(s"gdt_ad_log/$yesterday")
    val gdt_train_log2=sc.textFile(s"gdt_ad_log/$thedaybeforeyesterday")
    val gdt_train_log3=sc.textFile(s"gdt_ad_log/$threedaysago")

    val gdt_imei1=gdt_read_train_data(gdt_train_log1,spark).select("imei","title","desc","label")
    val gdt_imei2=gdt_read_train_data(gdt_train_log2,spark).select("imei","title","desc","label")
    val gdt_imei3=gdt_read_train_data(gdt_train_log3,spark).select("imei","title","desc","label")

    def pipei(adt:String,adc:String,label:String): String ={
      val all=adt+adc
      var arm=0
      for (i <- 1 until(gdt_map.size+1)){
        val f=gdt_map(i.toString)
        for (j<-f){
          if (all.contains(j)&arm==0){
            arm=i
          }
        }
      }
      arm.formatted("%02d")+label
    }

    def cut_lll(str:String):String={
      str.substring(0,2)
    }

    val cut_str: UserDefinedFunction =udf(cut_lll _)
    val gu: UserDefinedFunction =udf(pipei _)

    val gdt_imei=gdt_imei1.union(gdt_imei2).union(gdt_imei3).filter("imei is not null and title is not null and label is not null").filter(x=>x.getString(0).length>2)

    val gdt_f=gdt_imei.withColumn("lll",gu(col("title"),col("desc"),col("label"))).withColumn("ll",cut_str(col("lll"))).groupBy("imei","ll").agg(max("lll") as "id_int").groupBy("imei").agg(collect_list("id_int"))

    gdt_f.rdd.repartition(1).map{x=>
      val imei=x(0).toString
      val list=x.getSeq(1).mkString(",")
      imei+":"+list
    }.saveAsTextFile("/user/dm/huangyukun/gdt/ctr/imei_act.txt")
  }

  def mainn(args: Array[String]): Unit = {
    //val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://l-db14-1.prod.qd1.corp.agrant.cn:3306/ZCG_AD_DB", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "Ad", "user" -> "dev", "password" -> "vKITJVGT7dianJMXDNERlcK2zYEbVkutEShK69SFDxTlIJF3SjLlHCbhZcfw")).load()

    //val gdtdata =sc.textFile(s"/user/ssp/log/adshow*$yesterday*lzo")
    //val gdtdataclick =sc.textFile(s"/user/ssp/log/adclick*$yesterday*lzo")
    //val gdtDF=gdtdf(gdtdata)
    //val gdtDFclick=gdtdf(gdtdataclick)
    //val gdt5=gdtDF.filter("src==5 and adid is not null").drop("title","desc").withColumn("label",lit(0))
    //val gdt5click=gdtDFclick.filter("src==5 and adid is not null").drop("title","desc").withColumn("label",lit(1))
    //val gdt5all=gdt5.union(gdt5click)
    //jdbcDF.join(gdt5all,Seq("adid")).withColumn("r",randn()).sort("r").drop("r").rdd.map(line=>line.toSeq.mkString("||")).repartition(1).saveAsTextFile(s"gdt_ad_log/$yesterday")
  }
}