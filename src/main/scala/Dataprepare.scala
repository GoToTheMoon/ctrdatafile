import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


object Dataprepare extends Serializable{
  //bid数据app渠道
   val bidder_base = "/user/dsp/bidder_agg"
   val bidder_channel = "{adx,baidu,baidubn,gdt,hzx,jtx,xtx,tanx,winabn}/agg"
   val bidder_type_deviceid = "data/deviceid"
   val bidder_parquet = "data/warehouseMidReportParquet"
   //show数据
   val show_base = "/user/dsp/warehouse/logdata.db/show"
   //点击数据
   val click_base = "/user/dsp/warehouse/logdata.db/dsptt"
   //打标签
   def label_tag(positive_tag: String): Int = {
     val label = if (positive_tag == "1" ) 1 else 0
     label
   }
   //对底价进行处理
   def deal_min_cpm(min_cpm: Long): Int = {
     val cpm = min_cpm.toInt / 1000000
     cpm.toInt
   }

   /**
     *  获取ctr预估的正负样本
     * @param spark
     * @param sc
     * @param day_num
     */
   def get_click_data(spark:SparkSession,sc:SparkContext, day_num: Int): Unit= {
     val to_label: UserDefinedFunction = udf(label_tag _)
     val days = Tool.getRecentNDays_v2(day_num)
     val time_format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
     val time_string = time_format.format(new Date())
     println(s"开始正负样本处理数据$time_string...")
     for (day <- days) {
       val days_str = s"/dateline=$day"
       val show_data = spark.read.parquet(show_base + days_str)
         .filter("(pt like '10%') and reqid is not null")
         .select("reqid", "logtime", "month", "week", "dayofweek", "hour", "channel", "cid", "adgroupid", "tp", "planid", "svs", "baseprice")
         .filter(x => x.getString(0).size == 32)
         .withColumnRenamed("reqid", "request_id")
       show_data.createOrReplaceTempView("show_temp_data")
       val show_data_distinct  = spark.sql("select * from (select *,row_number() over(partition by request_id order by logtime) as logtime_row from show_temp_data ) t1 where t1.logtime_row = 1").drop("logtime","logtime_row")

       val click_data_distinct = spark.read.parquet(click_base + days_str)
         .filter("(pt like '10%') and reqid is not null")
         .selectExpr("reqid", "1 as positive_tag")
         .filter(x => x.getString(0).size == 32)
         .withColumnRenamed("reqid", "request_id")
         .distinct
       //串联正负样本
       val union_sample_data = show_data_distinct.join(click_data_distinct, Seq("request_id"), "left_outer")
         .withColumn("clicked", to_label(col("positive_tag"))).drop("positive_tag")
         .persist(StorageLevel.MEMORY_AND_DISK_SER)

       // val black_list = sc.textFile(s"/user/spider/ip/blacklist4bidder/ipblack.")
       //获取小时日志
       val year = day.substring(0,4)
       val month = day.substring(4,6)
       val dd = day.substring(6,8)
       val date_format = s"$year-$month-$dd"
       val date_str = Array(0 to 23).head.toArray.map(x => {
         val hour = x.toString
         val hour_str = if (hour.length == 1) s"0$hour" else hour
         s"$date_format-$hour_str"
       })
       for (data_hour_str <- date_str) {
         val bidder_log_path_hour = s"$bidder_base/$bidder_channel/$data_hour_str/$bidder_parquet"
         val bid_hour_data = spark.read.parquet(bidder_log_path_hour)
           .select("req", "site", "categoryid", "region", "width", "height", "stype", "position", "bkid", "mid", "dt", "pf", "min_cpm", "bklevel", "plantype", "grouptype", "nt", "browser", "kernel", "dtbrand", "dtmodel")
           .filter("req is not null")
           .filter(x => x.getString(0).size == 32)
           .withColumnRenamed("req", "request_id")

         val bid_union_sample_data = bid_hour_data.join(union_sample_data, Seq("request_id"), "inner")
         bid_union_sample_data.repartition(1).write.mode("overwrite").parquet(s"/user/dm/huangyukun/ctr/hour_basedata_pc/$data_hour_str")
         println(s"-------------------\t 写入点击的小时数据$data_hour_str")
       }
       union_sample_data.unpersist()
       val bid_day_data = spark.read.parquet(s"/user/dm/huangyukun/ctr/hour_basedata_pc/$date_format*")
       bid_day_data.repartition(10).write.mode("overwrite").parquet(s"/user/dm/huangyukun/ctr/day_basedata_pc/$date_format")
       println(s"-----------\t 写入天的点击数据$date_format")
     }
     val time_string_end = time_format.format(new Date())
     println(s"结束正负样本数据处理$time_string_end")
   }
 }