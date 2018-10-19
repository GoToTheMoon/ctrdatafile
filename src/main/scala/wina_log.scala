package looklike

import com.agrant.testapp.TestApp4
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json.JSONObject

object wina_log {
  val sparkConf = new SparkConf().setAppName("bid")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext
  val rowRDD3ago =sc.textFile("/user/dm/wina_log/ad_l-thirdclick*_prod_qd1_corp_agrant_cn_log.2018-10-16*.lzo")
  import spark.implicits._
  val reqlog3ago=rowRDD3ago.filter(x=>x.contains("params:")).map{ x=>
    val values=x.split("params:")
    val play_ad = new JSONObject(values(0).split("result:")(1)).getString("sign")
    val req_id = values(0).split("WinaInterface")(0).split("reqid:")(1).split("Instance")(0).trim
    val jsonstr = values(1)
    val jObject = new JSONObject(jsonstr)
    val solt_id = jObject.getString("solt_id")
    val adlist = jObject.getJSONArray("adList")

    var adc=""
    var adt=""
    var add=""
    for (i<-0 to adlist.length()-1){
      val jObject1 = new JSONObject(adlist.get(i).toString)
      val adid = jObject1.getString("ad_id")
      if (adid == play_ad) {
        val clickurl = jObject1.getString("click_url")
        val htt = clickurl.split("[.]")
        adt=jObject1.getString("ad_title")
        add=jObject1.get("ad_desc").toString
        if (htt(0).equals("https://cpro")) {
          adc="1"
        }else{
          if(htt(0).equals("http://cpro")){
            adc="2"
          }else{
            adc="3"
          }
        }
      }
    }
    (req_id,solt_id,adt,add,adc)
  }.toDF("req_id","solt_id","adt","add","adc")

  //拉到的广告
  val word="健身、瑜伽、运动鞋、运动服、运动裤、篮球、足球、羽毛球、乒乓球、匹克、彪马、匡威、贵人鸟、卡帕、七匹狼、锐步、特步、安踏、杰克琼斯、鸿星尔克、361、阿迪达斯、乔丹、美特斯邦威、耐克、李宁、冬奥会、中超、亚冠、国足、足协杯、中甲、国际、英超、西甲、欧冠、意甲、德甲、NBA、CBA、WCBA、男篮、女篮、NBL、马术、排球、网球、跑步、拳击、羽毛球、赛车、台球、棋牌、体操、曲棍球、射击、击剑、举重、时尚体育、帆船、赛艇、德州扑克、ONE冠军赛、奥运、F1、高尔夫、彩票"
  val wordss=word.split("、")
  var n=0
  var nn=0
  val reqlog3agoget=rowRDD3ago.filter(x=>x.contains("params:")).map{ x=>
    val values=x.split("params:")
    val play_ad = new JSONObject(values(0).split("result:")(1)).getString("sign")
    val req_id = values(0).split("WinaInterface")(0).split("reqid:")(1).split("Instance")(0).trim
    val jsonstr = values(1)
    val jObject = new JSONObject(jsonstr)
    val solt_id = jObject.getString("solt_id")
    val adlist = jObject.getJSONArray("adList")
    var adc=""
    var adt=""
    var add=""
    for (i<-0 to adlist.length()-1){
      nn+=1
      val jObject1 = new JSONObject(adlist.get(i).toString)
      val adid = jObject1.getString("ad_id")

        val clickurl = jObject1.getString("click_url")
        val htt = clickurl.split("[.]")
        adt=jObject1.getString("ad_title")
        add=jObject1.get("ad_desc").toString
      for (x<-wordss){if(adt.contains(x)||add.contains(x)){n+=1}}
    }
      (n,nn)
  }.toDF("req_id","l")

var ns=0

  val reqlog3agoget3=rowRDD3ago.filter(x=>x.contains("params:")).map{ x=>
    val values=x.split("params:")
    val play_ad = new JSONObject(values(0).split("result:")(1)).getString("sign")
    val req_id = values(0).split("WinaInterface")(0).split("reqid:")(1).split("Instance")(0).trim
    val jsonstr = values(1)
    val jObject = new JSONObject(jsonstr)
    val solt_id = jObject.getString("solt_id")
    val adlist = jObject.getJSONArray("adList")
    val adr_id = jObject.get("android_id").toString
    val appid=jObject.get("idfa").toString
    val id=adr_id + appid
    var adc=""
    var adt=""
    var add=""
    var flag=0
    var f=0

    for (i<-0 until adlist.length()){
      nn+=1
      f=1
      val jObject1 = new JSONObject(adlist.get(i).toString)
      val adid = jObject1.getString("ad_id")

      val clickurl = jObject1.getString("click_url")
      val htt = clickurl.split("[.]")
      adt=jObject1.getString("ad_title")
      add=jObject1.get("ad_desc").toString
      for (x<-wordss){if(adt.contains(x)||add.contains(x)){flag=1; ns+=1}}
    }
    (id,flag,f,ns,nn)
  }.toDF("id","ad","label","count","all").map{x=>
    val values=x.getString(0).split("params:")
    val jsonstr = values(1)
    val jObject = new JSONObject(jsonstr)
    val solt_id = jObject.get("android_id").toString
    val appid=jObject.get("idfa").toString
    solt_id + appid
  }.toDF("id")

  //点击率
  val xtoz=udf(TestApp4.xtozh _)
  val train23=spark.read.format("csv").option("header","true").option("delimiter", "|").csv("/user/dm/winafeed/data/app_dmp_2018100{7,8,9}.csv*").withColumn("adtitle",xtoz(col("ad_title"))).withColumn("addesc",xtoz(col("ad_desc"))).drop("ad_desc","ad_title")

  val train4=spark.read.format("csv").option("header","true").option("delimiter", "|").csv("/user/dm/winafeed/data/app_dmp_20181010.csv*").withColumn("adtitle",xtoz(col("ad_title"))).withColumn("addesc",xtoz(col("ad_desc"))).drop("ad_desc","ad_title")

  val word1="健身、瑜伽、运动鞋、运动服、运动裤、篮球、足球、羽毛球、乒乓球、匹克、彪马、匡威、贵人鸟、卡帕、七匹狼、锐步、特步、安踏、杰克琼斯、鸿星尔克、361、阿迪达斯、乔丹、美特斯邦威、耐克、李宁、冬奥会、中超、亚冠、国足、足协杯、中甲、国际、英超、西甲、欧冠、意甲、德甲、NBA、CBA、WCBA、男篮、女篮、NBL、马术、排球、网球、跑步、拳击、羽毛球、赛车、台球、棋牌、体操、曲棍球、射击、击剑、举重、时尚体育、帆船、赛艇、德州扑克、ONE冠军赛、奥运、F1、高尔夫、彩票"
  val words=word1.split("、")
  val tad23=train23.select("ad_req_id","adtitle","addesc","style","msg","imei","code_id").filter{x=>
    val x0=x(1).toString
    val x1=x(2).toString
    var g=false
    for (x<-words){if(x0.contains(x)||x1.contains(x)){g=true}}
    g
  }.cache
  tad23.filter("msg=='app_noticeUrl_click' and style=='flowinfo'").select("imei").distinct.rdd.map(line=>line.toSeq.mkString(",")).saveAsTextFile("wina_sport_imei.csv")
  tad23.filter("msg=='app_noticeUrl_click' and style=='flowinfo'").select("imei","code_id").distinct().groupBy("code_id").count().sort(col("count").desc).rdd.map(line=>line.toSeq.mkString(",")).repartition(1).saveAsTextFile("codeid_imeicount.csv")
  //青芒
  val trainclickqm=train23.filter("code_id=='nsoJjnF11041' and msg=='app_noticeUrl_click'")
  trainclickqm.select($"ad_req_id" as "req_id").withColumn("label",lit(1)).join(reqlog3ago,Seq("req_id")).groupBy("adc","solt_id").count.show
  val trainshowqm=train23.filter("code_id=='nsoJjnF11041' and msg=='app_noticeUrl_callback' and style=='flowinfo'")
  trainshowqm.select($"ad_req_id" as "req_id").withColumn("label",lit(1)).join(reqlog3ago,Seq("req_id")).groupBy("adc","solt_id").count.show

  //全点击
  val train22=spark.read.format("csv").option("header","true").option("delimiter", "|").csv("/user/dm/winafeed/data/app_dmp_20180925.csv*")
  val trainclickall=train22.filter("msg=='app_noticeUrl_callback' and style=='flowinfo'")
  trainclickall.select($"ad_req_id" as "req_id").join(reqlog3ago,Seq("req_id")).select("adt","add","req_id").filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    var g=false
    for (x<-words){if(x0.contains(x)||x1.contains(x)){g=true}}
    g
  }.count
  val trainshowall=train22.filter("msg=='app_noticeUrl_callback' and style=='flowinfo'")
  trainshowall.select($"ad_req_id" as "req_id").withColumn("label",lit(1)).join(reqlog3ago,Seq("req_id")).groupBy("adc","solt_id").count.show(50)

  val reqlogfirst3ago=rowRDD3ago.filter(x=>x.contains("params:")).map{ x=>
    val values=x.split("params:")
    val play_ad = values(0).split("result:")(1).substring(2).split(",|]")(0)
    val req_id = values(0).split("WinaInterface")(0).split("reqid:")(1).split("Instance")(0).trim
    val jsonstr = values(1)
    val jObject = new JSONObject(jsonstr)
    val solt_id = jObject.getString("solt_id")
    val adlist = jObject.getJSONArray("adList")

    var adc=""
    var adt=""

    val jObject1 = new JSONObject(adlist.get(0).toString)
    val adid = jObject1.getString("ad_id")

    val clickurl = jObject1.getString("click_url")
    val htt = clickurl.split("[.]")
    adt=jObject1.getString("ad_title")
    if (htt(0).equals("https://cpro")) {
      adc="1"
    }else{
      if(htt(0).equals("http://cpro")){
        adc="2"
      }else{
        adc="3"
      }
    }
    (req_id,solt_id,adt,adc)
  }.toDF("req_id","solt_id","adt","adc")


  val geust24=rowRDD3ago.filter(x=>x.contains("params:")).map{ x=>
    val values=x.split("params:")

    val jsonstr = values(1)
    val jObject = new JSONObject(jsonstr)
    val solt_id = jObject.getString("solt_id")
    val imei = jObject.getString("imei")

    (solt_id,imei)
  }.toDF("solt_id","imei")





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

  def gdtdf(rDD: RDD[String]):DataFrame={
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

  import org.apache.spark.sql.SQLContext
  val sqlContext = new SQLContext(sc)
  val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://l-db14-1.prod.qd1.corp.agrant.cn:3306/ZCG_AD_DB", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> "Ad", "user" -> "dev", "password" -> "vKITJVGT7dianJMXDNERlcK2zYEbVkutEShK69SFDxTlIJF3SjLlHCbhZcfw")).load()

  val gdtdata =sc.textFile("/user/ssp/log/adshow*2018-08-24*lzo")
  val gdtdataclick =sc.textFile("/user/ssp/log/adclick*2018-08-24*lzo")
  val gdtDF=gdtdf(gdtdata)
  val gdtDFclick=gdtdf(gdtdataclick)
  val gdt5=gdtDF.filter("src==5 and adid is not null").drop("title","desc").withColumn("label",lit(0))
  val gdt5click=gdtDFclick.filter("src==5 and adid is not null").drop("title","desc").withColumn("label",lit(1))
  val gdt5all=gdt5.union(gdt5click)
  jdbcDF.join(gdt5all,Seq("adid")).withColumn("r",randn()).sort("r").drop("r").rdd.map(line=>line.toSeq.mkString("||")).repartition(1).saveAsTextFile("gdt_ad_log/2018-08-24")


  //read train data
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

  def gdt_read_train_data(rDD: RDD[String]):DataFrame={
    val gdt=rDD.filter(x=>x.split("\\|\\|").length>2).map{x=>
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

 val gdt_train_log16=sc.textFile("gdt_ad_log/2018-08-25")

  val gdt_data16=gdt_read_train_data(gdt_train_log16)

  val gdt_click1=gdt_data16.filter("label=1 and src=5")

  val ad_distinct=gdt_data16.filter("appid='APP18050234694094'").select("imei","title","desc","label")

  val ad_not_eat=ad_distinct.filter { x =>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("美食")|x0.contains("大众点评")|x0.contains("必胜客")|x0.contains("外卖")|x0.contains("龙虾")|x0.contains("吃饭")|x0.contains("美团")|x0.contains("哈根达斯")|x1.contains("美团")|x1.contains("哈根达斯")|x1.contains("外卖")|x1.contains("龙虾")|x1.contains("吃饭")|x1.contains("必胜客")|x1.contains("大众点评")|x1.contains("美食"))
  }

  ad_not_eat.groupBy("label").count.show
  val ad_not_watch=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("B612咔叽")|x0.contains("KilaKila")|x0.contains("爱奇艺")|x0.contains("虎牙")|x0.contains("拍照")|x0.contains("美女")|x0.contains("YY")|x0.contains("抖音")|x0.contains("视频")|x0.contains("直播")|x0.contains("快手")|x0.contains("微视")|x0.contains("陌陌")|x1.contains("抖音")|x1.contains("视频")|x1.contains("直播")|x1.contains("快手")|x1.contains("微视")|x1.contains("陌陌")|x1.contains("YY")|x1.contains("美女")|x1.contains("拍照")|x1.contains("爱奇艺")|x1.contains("虎牙")|x1.contains("KilaKila")|x1.contains("B612咔叽"))
  }

  ad_not_watch.groupBy("label").count.show
  val ad_not_read=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("最右")|x0.contains("咚漫")|x0.contains("浙江24小时")|x0.contains("趣头条")|x0.contains("书")|x0.contains("天天快报")|x0.contains("读书")|x0.contains("聚看点")|x0.contains("今日头条")|x0.contains("搜一搜")|x0.contains("小说")|x0.contains("知乎")|x0.contains("阅读")|x0.contains("新闻")|x0.contains("漫画")|x0.contains("动漫")|x1.contains("知乎")|x1.contains("小说")|x1.contains("阅读")|x1.contains("新闻")|x1.contains("漫画")|x1.contains("动漫")|x1.contains("搜一搜")|x1.contains("今日头条")|x1.contains("聚看点")|x1.contains("天天快报")|x1.contains("读书")|x1.contains("书")|x1.contains("趣头条")|x1.contains("浙江24小时")|x1.contains("咚漫")|x1.contains("最右"))
  }

  ad_not_read.groupBy("label").count.show
  val ad_not_live=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("链家")|x0.contains("搬家")|x0.contains("房价")|x0.contains("房多多")|x0.contains("房子")|x0.contains("中介")|x0.contains("58同城")|x0.contains("买房")|x0.contains("二手房")|x0.contains("贝壳")|x1.contains("二手房")|x1.contains("贝壳")|x1.contains("买房")|x1.contains("58同城")|x1.contains("房子")|x1.contains("中介")|x1.contains("房价")|x1.contains("房多多")|x1.contains("搬家")|x1.contains("链家"))
  }

  ad_not_live.groupBy("label").count.show
  val ad_not_travel=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("出行")|x0.contains("短租")|x0.contains("途牛")|x0.contains("外景房")|x0.contains("海景房")|x0.contains("民居")|x0.contains("婚礼纪")|x0.contains("航班")|x0.contains("出游")|x0.contains("旅行")|x0.contains("机票")|x0.contains("酒店")|x0.contains("携程")|x0.contains("去哪儿")|x0.contains("游轮")|x0.contains("电影")|x1.contains("旅行")|x1.contains("出游")|x1.contains("机票")|x1.contains("酒店")|x1.contains("携程")|x1.contains("去哪儿")|x1.contains("游轮")|x1.contains("电影")|x1.contains("航班")|x1.contains("婚礼纪")|x1.contains("外景房")|x1.contains("海景房")|x1.contains("民居")|x1.contains("途牛")|x1.contains("出行")|x1.contains("短租"))
  }

  ad_not_travel.groupBy("label").count.show
  val ad_not_buy=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("购物")|x0.contains("小米有品")|x0.contains("天猫")|x0.contains("醒购")|x0.contains("闪送")|x0.contains("鞋")|x0.contains("特卖")|x0.contains("有货")|x0.contains("每日优鲜")|x0.contains("内裤")|x0.contains("内衣")|x0.contains("手表")|x0.contains("二折")|x0.contains("三折")|x0.contains("四折")|x0.contains("五折")|x0.contains("六折")|x0.contains("七折")|x0.contains("运动")|x0.contains("聚美优品")|x0.contains("优衣库")|x0.contains("搭配")|x0.contains("蘑菇街")|x0.contains("女装")|x0.contains("衣服")|x0.contains("穿搭")|x0.contains("折800")|x0.contains("优惠")|x0.contains("一折")|x0.contains("9折")|x0.contains("8折")|x0.contains("7折")|x0.contains("6折")|x0.contains("5折")|x0.contains("4折")|x0.contains("3折")|x0.contains("2折")|x0.contains("1折")|x0.contains("空调")|x0.contains("电视")|x0.contains("车")|x0.contains("电脑")|x0.contains("抢购")|x0.contains("百搭")|x0.contains("唯品会")|x0.contains("清仓")|x0.contains("特惠")|x0.contains("满减")|x0.contains("爆款")|x0.contains("京东")|x0.contains("淘宝")|x0.contains("苏宁")|x0.contains("转转")|x0.contains("拼多多")|x0.contains("降价")|x0.contains("信用卡")|x0.contains("下单")|x0.contains("付款")|x0.contains("多点")|x0.contains("实惠")|x0.contains("划算")|x0.contains("特价")|x1.contains("京东")|x1.contains("淘宝")|x1.contains("苏宁")|x1.contains("转转")|x1.contains("拼多多")|x1.contains("降价")|x1.contains("信用卡")|x1.contains("下单")|x1.contains("付款")|x1.contains("多点")|x1.contains("实惠")|x1.contains("划算")|x1.contains("特价")|x1.contains("满减")|x1.contains("爆款")|x1.contains("清仓")|x1.contains("特惠")|x1.contains("唯品会")|x1.contains("百搭")|x1.contains("空调")|x1.contains("电视")|x1.contains("车")|x1.contains("电脑")|x1.contains("抢购")|x1.contains("9折")|x1.contains("8折")|x1.contains("7折")|x1.contains("6折")|x1.contains("5折")|x1.contains("4折")|x1.contains("3折")|x1.contains("2折")|x1.contains("1折")|x1.contains("一折")|x1.contains("优惠")|x1.contains("折800")|x1.contains("穿搭")|x1.contains("衣服")|x1.contains("女装")|x1.contains("蘑菇街")|x1.contains("搭配")|x1.contains("运动")|x1.contains("聚美优品")|x1.contains("优衣库")|x1.contains("二折")|x1.contains("三折")|x1.contains("四折")|x1.contains("五折")|x1.contains("六折")|x1.contains("七折")|x1.contains("手表")|x1.contains("内裤")|x1.contains("内衣")|x1.contains("每日优鲜")|x1.contains("有货")|x1.contains("特卖")|x1.contains("鞋")|x1.contains("醒购")|x1.contains("闪送")|x1.contains("购物")|x1.contains("小米有品")|x1.contains("天猫"))
  }

  ad_not_buy.groupBy("label").count.show
  val ad_not_game=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("放置奇兵")|x0.contains("副本")|x0.contains("地下城")|x0.contains("电竞")|x0.contains("挂机")|x0.contains("排位")|x0.contains("段位")|x0.contains("神兵")|x0.contains("怪物")|x0.contains("将军")|x0.contains("神将")|x0.contains("TapTap")|x0.contains("策略")|x0.contains("魔幻")|x0.contains("西游")|x0.contains("元宝")|x0.contains("中国区")|x0.contains("异兽")|x0.contains("乱世")|x0.contains("指点江山")|x0.contains("权御天下")|x0.contains("模拟")|x0.contains("国服")|x0.contains("中国区")|x0.contains("神兽")|x0.contains("觉醒")|x0.contains("角色")|x0.contains("游戏")|x0.contains("兵种")|x0.contains("金币")|x0.contains("技能")|x0.contains("升级")|x0.contains("装备")|x0.contains("召回")|x0.contains("召唤")|x0.contains("传奇")|x0.contains("三国")|x0.contains("开局")|x0.contains("手游")|x0.contains("单机")|x0.contains("投胎")|x0.contains("神装")|x0.contains("英雄")|x0.contains("红警")|x0.contains("基地")|x0.contains("重回")|x0.contains("重生")|x0.contains("组队")|x0.contains("战力") |x1.contains("装备")|x1.contains("召回")|x1.contains("召唤")|x1.contains("传奇")|x1.contains("三国")|x1.contains("开局")|x1.contains("手游")|x1.contains("单机")|x1.contains("投胎")|x1.contains("神装")|x1.contains("英雄")|x1.contains("红警")|x1.contains("基地")|x1.contains("重回")|x1.contains("重生")|x1.contains("组队")|x1.contains("战力")|x1.contains("技能")|x1.contains("升级")|x1.contains("金币")|x1.contains("神兽")|x1.contains("觉醒")|x1.contains("角色")|x1.contains("游戏")|x1.contains("兵种")|x1.contains("国服")|x1.contains("中国区")|x1.contains("模拟")|x1.contains("权御天下")|x1.contains("指点江山")|x1.contains("中国区")|x1.contains("异兽")|x1.contains("乱世")|x1.contains("西游")|x1.contains("元宝")|x1.contains("魔幻")|x1.contains("策略")|x1.contains("神将")|x1.contains("将军")|x1.contains("神兵")|x1.contains("怪物")|x1.contains("放置奇兵")|x1.contains("副本")|x1.contains("地下城")|x1.contains("电竞")|x1.contains("挂机")|x1.contains("排位")|x1.contains("段位"))
  }

  ad_not_game.groupBy("label").count.show
  val ad_not_func=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("浏览器")|x0.contains("WIFI")|x0.contains("网速")|x0.contains("FM")|x0.contains("高德地图")|x0.contains("微云")|x0.contains("天气")|x0.contains("支付宝")|x0.contains("360")|x0.contains("百度")|x0.contains("宽带")|x0.contains("营业厅")|x0.contains("流量")|x0.contains("随手记")|x1.contains("360")|x1.contains("百度")|x1.contains("宽带")|x1.contains("营业厅")|x1.contains("流量")|x1.contains("随手记")|x1.contains("支付宝")|x1.contains("微云")|x1.contains("天气")|x1.contains("高德地图")|x1.contains("FM")|x1.contains("UC浏览器")|x1.contains("WIFI")|x1.contains("网速"))
  }

  ad_not_func.groupBy("label").count.show
  val ad_not_money=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("放款")|x0.contains("抵押")|x0.contains("月息")|x0.contains("工作")|x0.contains("boss")|x0.contains("36氪")|x0.contains("直聘")|x0.contains("盯盘")|x0.contains("借钱")|x0.contains("到账")|x0.contains("微鲤看看")|x0.contains("股票")|x0.contains("原石")|x0.contains("工资")|x0.contains("创业")|x0.contains("炒股")|x0.contains("红包")|x0.contains("贷款")|x0.contains("还款")|x0.contains("薪资")|x0.contains("零花钱")|x0.contains("借款")|x0.contains("高薪")|x0.contains("斗米")|x0.contains("概念股")|x0.contains("高利贷")|x0.contains("应聘")|x0.contains("入职")|x1.contains("红包")|x1.contains("贷款")|x1.contains("还款")|x1.contains("薪资")|x1.contains("零花钱")|x1.contains("借款")|x1.contains("高薪")|x1.contains("斗米")|x1.contains("概念股")|x1.contains("高利贷")|x1.contains("应聘")|x1.contains("入职")|x1.contains("炒股")|x1.contains("工资")|x1.contains("创业")|x1.contains("原石")|x1.contains("股票")|x1.contains("借钱")|x1.contains("到账")|x1.contains("微鲤看看")|x1.contains("盯盘")|x1.contains("直聘")|x1.contains("36氪")|x1.contains("工作")|x1.contains("boss")|x1.contains("月息")|x1.contains("放款")|x1.contains("抵押"))
  }

  ad_not_money.groupBy("label").count.show
  val ad_not_edu=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("辅导")|x0.contains("教程")|x0.contains("考证")|x0.contains("培训")|x0.contains("口语")|x0.contains("单词")|x0.contains("编程")|x0.contains("西点")|x0.contains("美术")|x0.contains("烹饪")|x0.contains("学校")|x0.contains("培养")|x0.contains("蜕变")|x0.contains("自信")|x0.contains("学历")|x0.contains("大学")|x0.contains("教学")|x0.contains("学习")|x0.contains("老师")|x0.contains("本科")|x0.contains("教育")|x0.contains("英语")|x1.contains("本科")|x1.contains("教育")|x1.contains("英语")|x1.contains("老师")|x1.contains("培养")|x1.contains("蜕变")|x1.contains("自信")|x1.contains("学历")|x1.contains("大学")|x1.contains("教学")|x1.contains("学习")|x1.contains("烹饪")|x1.contains("学校")|x1.contains("美术")|x1.contains("西点")|x1.contains("编程")|x1.contains("单词")|x1.contains("口语")|x1.contains("教程")|x1.contains("考证")|x1.contains("培训")|x1.contains("辅导"))
  }

  ad_not_edu.groupBy("label").count.show
  val ad_not_self=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("洗发")|x0.contains("显老")|x0.contains("相亲")|x0.contains("防晒")|x0.contains("单身")|x0.contains("肌肤")|x0.contains("减肥")|x0.contains("Keep")|x0.contains("近视")|x0.contains("牙齿")|x0.contains("头发")|x0.contains("发油")|x0.contains("失眠")|x0.contains("皱纹")|x0.contains("法令纹")|x0.contains("面膜")|x0.contains("减压")|x0.contains("眼睛")|x0.contains("身高")|x0.contains("长高")|x0.contains("眼镜")|x0.contains("茶")|x0.contains("保健")|x0.contains("健身")|x1.contains("头发")|x1.contains("发油")|x1.contains("失眠")|x1.contains("皱纹")|x1.contains("法令纹")|x1.contains("面膜")|x1.contains("减压")|x1.contains("眼睛")|x1.contains("身高")|x1.contains("长高")|x1.contains("眼镜")|x1.contains("茶")|x1.contains("保健")|x1.contains("健身")|x1.contains("牙齿")|x1.contains("近视")|x1.contains("Keep")|x1.contains("相亲")|x1.contains("防晒")|x1.contains("单身")|x1.contains("肌肤")|x1.contains("减肥")|x1.contains("洗发")|x1.contains("显老")|x1.contains("社交")|x1.contains("技巧")|x1.contains("美容")|x1.contains("恋爱")|x1.contains("心动")|x1.contains("浪漫")|x1.contains("表白"))
  }

  ad_not_self.groupBy("label").count.show
  val ad_not_fam=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("童装")|x0.contains("疾险")|x0.contains("宝妈")|x0.contains("病险")|x0.contains("孩子")|x0.contains("宝宝")|x0.contains("炒菜")|x0.contains("电饭煲")|x0.contains("洁净")|x0.contains("洗碗")|x0.contains("家居")|x0.contains("家具")|x0.contains("保险")|x0.contains("平安")|x0.contains("营养")|x0.contains("厨房")|x0.contains("尿不湿")|x0.contains("人寿")|x0.contains("母婴")|x0.contains("奶粉")|x0.contains("奶爸")|x0.contains("美赞臣")|x0.contains("装修")|x0.contains("婴儿")|x0.contains("妈妈")|x1.contains("母婴")|x1.contains("奶粉")|x1.contains("奶爸")|x1.contains("美赞臣")|x1.contains("装修")|x1.contains("婴儿")|x1.contains("妈妈")|x1.contains("保险")|x1.contains("平安")|x1.contains("营养")|x1.contains("厨房")|x1.contains("尿不湿")|x1.contains("人寿")|x1.contains("家居")|x1.contains("家具")|x1.contains("洁净")|x1.contains("洗碗")|x1.contains("炒菜")|x1.contains("电饭煲")|x1.contains("宝宝")|x1.contains("孩子")|x1.contains("病险")|x1.contains("宝妈")|x1.contains("疾险")|x1.contains("童装"))
  }

  ad_not_fam.groupBy("label").count.show

  val ad_not_con=ad_distinct.filter{x=>
    val x0=x(0).toString
    val x1=x(1).toString
    (x0.contains("社交")|x0.contains("hello")|x0.contains("邻家")|x0.contains("探探")|x0.contains("社区")|x0.contains("单身")|x0.contains("相亲")|x0.contains("恋爱")|x0.contains("心动")|x0.contains("浪漫")|x0.contains("表白")|x0.contains("同城约")|x0.contains("约会吧")|x0.contains("邻友")|x1.contains("hello")|x1.contains("邻家")|x1.contains("探探")|x1.contains("社区")|x1.contains("单身")|x1.contains("相亲")|x1.contains("恋爱")|x1.contains("心动")|x1.contains("浪漫")|x1.contains("表白")|x1.contains("同城约")|x1.contains("约会吧")|x1.contains("邻友")|x1.contains("社交"))
  }

  ad_not_con.groupBy("label").count.show

  ad_distinct.filter{x=>
    val x0=x(0).toString+x(1).toString
    x0.contains("斗鱼")}.groupBy("label").count.show

 val ad_mdic=reqlog3ago.filter("solt_id=5890645 and adc!=3").filter{x=>
   val x0=x(2).toString
   val x1=x(3).toString
   (x0.contains("整形")|x0.contains("整容")|x0.contains("下巴")|x0.contains("眼皮")|x0.contains("瘦脸")|x0.contains("鼻子")|x0.contains("减肥")|x0.contains("抽脂")|x0.contains("瘦腿")|x0.contains("鼻尖")|x0.contains("磨骨")|x0.contains("眼角")|x0.contains("眼袋")|x0.contains("强直")|x0.contains("假体")|x0.contains("眉毛")|x0.contains("减脂")|x0.contains("祛斑")|x0.contains("除疤")|x0.contains("隆鼻")|x0.contains("鼻头")|x0.contains("吸脂")|x0.contains("额头")|x0.contains("皱纹")|x1.contains("整形")|x1.contains("整容")|x1.contains("下巴")|x1.contains("眼皮")|x1.contains("瘦脸")|x1.contains("鼻子")|x1.contains("减肥")|x1.contains("抽脂")|x1.contains("瘦腿")|x1.contains("鼻尖")|x1.contains("磨骨")|x1.contains("眼角")|x1.contains("眼袋")|x1.contains("强直")|x1.contains("假体")|x1.contains("眉毛")|x1.contains("减脂")|x1.contains("祛斑")|x1.contains("除疤")|x1.contains("隆鼻")|x1.contains("鼻头")|x1.contains("吸脂")|x1.contains("额头")|x1.contains("皱纹"))
 }

  val train_1=spark.read.format("csv").option("header","true").csv("/user/dm/zhouyonglong/context_ads/wina/sem/seedwords.csv")

  def groupprice(s:String):Int={
    val u=s.toDouble
    u match {
      case u if u<=5=>0
      case u if 5<u && u<=10=>1
      case u if 10<u && u<=15=>2
      case u if 15<u && u<=20=>3
      case u if 20<u && u<=25=>4
      case u if 25<u && u<=30=>5
      case u if 30<u && u<=35=>6
      case u if 35<u && u<=40=>7
      case u if 40<u && u<=45=>8
      case u if 45<u && u<=50=>9
      case u if u>50=>10
    }
  }

  val gt=udf(groupprice _)
  val train2=train_1.withColumn("c",gt(col("recBid")))
}