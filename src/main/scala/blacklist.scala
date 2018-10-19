import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._

class blacklist {
  val sparkConf = new SparkConf().setAppName("bid")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext
  def getLastWeekDateFormat1(days:Int)={
    val calendar = Calendar.getInstance();
    val str = new StringBuffer("{")
    for (i<-0 until  days) {
      calendar.add(Calendar.DATE, -1)
      val df = new SimpleDateFormat("yyyy-MM-dd")
      val date = df.format(calendar.getTime())
      str.append(date)
      str.append(",")
    }
    str.toString.dropRight(1)+"}"
  }
  val day = getLastWeekDateFormat1(1)
  val calendar = Calendar.getInstance()
  calendar.add(Calendar.DATE, -1)
  val df = new SimpleDateFormat("yyyyMMdd")
  val yesterday = df.format(calendar.getTime())
  val path = s"/user/dm/huangyukun/applog"
  val bidderLogPath = s"/user/dsp/bidder_agg/{baiduapp,tanxapp,baidufeed,gdtapp}/agg/$day-*/data/warehouseMidReportParquet"
  val bidpath="/user/dsp/bidder_agg/{gdtapp}/agg/2018-10-10-10/data/warehouseMidReportParquet"
  val data1 = spark.read.parquet(bidpath)
  val appSet = data1.select("site").map(x=>{
    val app = x.getString(0)
    (app,1)
  }).rdd.reduceByKey(_+_).filter(_._2>5000).map(_._1).collect().toSet

  val appdata=data1.select("deviceid","site","time").filter(x=>{
    val site = x.getString(1)
    appSet.contains(site)
  })

  //
  val temsample=appdata.sample(withReplacement = false,0.001).select("deviceid","site").cache
  appdata.join(temsample.distinct(),Seq("deviceid","site")).write.parquet("balckapplog")
  val temsamplelog=spark.read.parquet("blackapplog")
  val blacklist=new ArrayBuffer[String]()
  for (x<-appSet){
    var T=0
    var F=0
    val applog=temsamplelog.filter(s"site='$x'").cache()
    val appsample=temsample.filter(s"site='$x'")
    val appdevice=appsample.select("deviceid").collectAsList()
    for (y<-0 until appdevice.size()){
      val id=appdevice.get(y).getString(0)
      val idlogtime=applog.filter(s"deviceid='$id'").select("time").sort("time").collectAsList()
      val waittime=new Array[Double](idlogtime.size()-1)
      for (z<-0 until(idlogtime.size()-1)){
        val sdf = new SimpleDateFormat("yyyy-MM-dd H:mm:ss")
        val before=sdf.parse(idlogtime.get(z).getString(0)).getTime
        val last=sdf.parse(idlogtime.get(z+1).getString(0)).getTime
        val minus=last-before
        waittime(z)=minus
      }
      val waitset=waittime.toSet
      if(waitset.size<=0.5*(waittime.length)){F+=1}else{T+=1}
    }
    if(F>T){blacklist+=x}
  }
  sc.parallelize(blacklist).saveAsTextFile("blacklist")

  //
  appdata.write.parquet("blackapplog")
  val applog=spark.read.parquet("blackapplog")
  val appidlog=applog.distinct().groupBy("deviceid","site").agg(collect_list("time") as "times")
  val sdf = new SimpleDateFormat("yyyy-MM-dd H:mm:ss")
  val rt1=appidlog.map{x=>
    var flag=0
    val t=x.getSeq(2)
    if (t.lengthCompare(1) > 0){
      val minus=new Array[Double](t.length-1)
      val times=new Array[Double](t.length)
      for (i<-t.indices){
        times(i)=sdf.parse(t(i)).getTime
      }
      val tsort=times.sorted
      for (i <- 0 until tsort.length-1){
        minus(i)=tsort(i+1)-tsort(i)
      }
      val minusdis=minus.toSet
      val frate=minusdis.size/minus.length.toDouble
      if(frate<=0.5){flag=1}
    }
    (x.getString(0),x.getString(1),flag)
  }.toDF("deviceid","site","flag").cache
  rt1.groupBy("site").agg(sum("flag")/count("flag") as "rate").filter("rate>0.7").select("site").rdd.map(line=>line.toSeq.mkString(",")).saveAsTextFile("blacklisttrue")

  def g(s:String):String={
    s.replace("[","").replace("]","")
  }
  val gt=udf(g _)


  val site2idCount = templog.rdd.map(x=>{
    (x.getString(1),1)
  }).reduceByKey(_+_).collectAsMap()
  val rdd = templog.map(x=>{
    (x.getString(0),x.getString(1))
  }).rdd
  val r1 = rdd.join(rdd).filter(x=>x._2._1!=x._2._2).map(x=>{
    val (id,(app1,app2)) = x
    ((app1,app2),1)
  }).reduceByKey(_+_)
  val appBlackFromLog = r1.flatMap(x=>{
    val ((app1,app2),idcount) = x
    val  str = if(app1>app2) app1+"\t"+app2+"\t"+idcount+"\t"+site2idCount.getOrElse(app1,0)+"\t"+site2idCount.getOrElse(app2,0)
    else app2+"\t"+app1+"\t"+idcount+"\t"+site2idCount.getOrElse(app2,0)+"\t"+site2idCount.getOrElse(app1,0)
    val arr = str.split("\t")
    val allcount = arr(2).toInt //共有的id
    val app1Cover = allcount/(arr(3).toInt*1.00)
    val app2Cover = allcount/(arr(4).toInt*1.00)
    val res = new ArrayBuffer[String]()
    if(app1Cover>=0.5&&app2Cover>=0.5&&arr(3)!="1"&&arr(4)!="1"){res+=arr(0);res+=arr(1)}
    else if(app1Cover>=0.5&&app2Cover<0.5&&arr(3)!="1"){res+=arr(0)}
    else if(app1Cover<0.5&&app2Cover>=0.5&&arr(4)!="1"){res+=arr(1)}
    else if(app1Cover<0.5&&app2Cover<0.5){}
    //      if(app1Cover>=0.5&&app2Cover>=0.5){res+=str}
    //      else if(app1Cover>=0.5&&app2Cover<0.5){res+=str}
    //      else if(app1Cover<0.5&&app2Cover>=0.5){res+=str}
    //      else if(app1Cover<0.5&&app2Cover<0.5){}
    res
  }).distinct(60)
  import java.sql.{Connection, DriverManager, ResultSet}

  /**
    * Created by linsong.chen on 2016/12/6.
    */
  object MysqlUtil {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://l-db1-1.prod.cn2.corp.agrant.cn:3306/Adx?useUnicode=true&characterEncoding=UTF8"
    val username = "dmteam"
    val password = "2Ye7mDoJKaXbsbF8cIX4M13dMhvk0+utafLOZ65+O2Vd"

    def getConn(): Connection = {
      var connection:Connection = null
      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
      } catch {
        case e:Exception => println("wrong to get connection \n" +e.getMessage )
      }
      connection
    }
    def insertsql(sql: String) {
      var connection = getConn()
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          val resultSet = statement.executeUpdate(sql)
        } catch {
          case e:Exception => println(s"wrong to insert sql:$sql \n"+e.getMessage)
        }
      }
      connection.close()
    }
    def query(sql:String):ResultSet = {
      var connection = getConn()
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          val resultSet = statement.executeQuery(sql)
          return resultSet
        } catch {
          case e:Exception => println("wrong to query sql \n"+e.getMessage)
        }
      }
      connection.close()
      null
    }
    def update(sql:String,connection: Connection):Int = {
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          val resultSet = statement.executeUpdate(sql)
          return resultSet
        } catch {
          case e:Exception => println("wrong to query sql \n"+e.getMessage)
        }
      }
      1
    }
    def insertArraysqls(sqls:Array[String]){
      var connection = getConn()
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          sqls.foreach(sql=>{
            statement.executeUpdate(sql)
          })
        } catch {
          case e:Exception => println("wrong to insert sql list \n"+e.getMessage)
        }
      }
      connection.close()
    }
  }
  val res = MysqlUtil.query("select count(*) as rowCount from Adx.AppBase where dateline like DATE_FORMAT(CURDATE(),'%Y%m%d') and (appname = '' or recentupdatetime not like CONCAT(DATE_FORMAT(NOW(),'%Y'),'%'))")
  res.next();
  val size  = res.getInt("rowCount")
  val appBlack = new Array[String](size)
  val resultSet = MysqlUtil.query("SELECT appid FROM Adx.AppBase where dateline like DATE_FORMAT(CURDATE(),'%Y%m%d') and (appname = '' or recentupdatetime not like CONCAT(DATE_FORMAT(NOW(),'%Y'),'%'))")
  val arrayBuffer = new ArrayBuffer[String]()
  val tempAppBlack = appBlack.map(x=>{
    resultSet.next()
    val appid = resultSet.getString("appid")
    appid
  })
  val appBlackRdd = sc.parallelize(tempAppBlack.toSeq)

  val calendar1 = Calendar.getInstance()
  val df1 = new SimpleDateFormat("yyyy-MM-dd-HH")
  val now = df1.format(calendar1.getTime())
  appBlackRdd.union(appBlackFromLog).filter(appid=>appid!="youku_app"&&appid!="netease_newsreader_android.apk"&&appid!="com.android.browser"&&appid.size>0).saveAsTextFile(s"/user/dm/spider/app/blacklist4bidder/appblack.$now")



  //

  import java.text.SimpleDateFormat
  import java.util.Calendar
  import scala.collection.mutable.ArrayBuffer
  def getLastWeekDateFormat1(days:Int)={
    val calendar = Calendar.getInstance();
    val str = new StringBuffer("{")
    for (i<-0 until  days) {
      calendar.add(Calendar.DATE, -1)
      val df = new SimpleDateFormat("yyyy-MM-dd")
      val date = df.format(calendar.getTime())
      str.append(date)
      str.append(",")
    }
    str.toString.dropRight(1)+"}"
  }
  val day = getLastWeekDateFormat1(3)
  val calendar = Calendar.getInstance();
  calendar.add(Calendar.DATE, -1)
  val df = new SimpleDateFormat("yyyyMMdd")
  val yesterday = df.format(calendar.getTime())
  val path = s"/user/dm/temp/yesterday"
  val bidderLogPath = s"/user/dsp/bidder_agg/{baiduapp,tanxapp,baidufeed,gdtapp}/agg/$day-*/data/warehouseMidReportParquet"
  val data1 = spark.read.parquet(bidderLogPath).select("site")
  val appSet = data1.map(x=>{
    val app = x.getString(0)
    (app,1)
  }).rdd.reduceByKey(_+_).filter(_._2>300000).map(_._1).collect().toSet
  spark.read.parquet(bidderLogPath).select("deviceid","site","usertagstr").filter(x=>{
    val deviceid  =  x.getString(0)
    val site = x.getString(1)
    val usertagstr =  x.getString(2)
    appSet.contains(site)
  }).select("deviceid","site").write.mode("overwrite").parquet(path)
  val applog1 =  applog.select("deviceid","site").distinct
  val site2idCount = applog1.rdd.map(x=>{
    (x.getString(1),1)
  }).reduceByKey(_+_).collectAsMap()
  val rdd = applog1.map(x=>{
    (x.getString(0),x.getString(1))
  }).rdd
  val r1 = rdd.join(rdd).filter(x=>x._2._1!=x._2._2).map(x=>{
    val (id,(app1,app2)) = x
    ((app1,app2),1)
  }).reduceByKey(_+_)
  val appBlackFromLog = r1.flatMap(x=>{
    val ((app1,app2),idcount) = x
    val  str = if(app1>app2) app1+"\t"+app2+"\t"+idcount+"\t"+site2idCount.getOrElse(app1,0)+"\t"+site2idCount.getOrElse(app2,0)
    else app2+"\t"+app1+"\t"+idcount+"\t"+site2idCount.getOrElse(app2,0)+"\t"+site2idCount.getOrElse(app1,0)
    val arr = str.split("\t")
    val allcount = arr(2).toInt //共有的id
    val app1Cover = allcount/(arr(3).toInt*1.00)
    val app2Cover = allcount/(arr(4).toInt*1.00)
    val res = new ArrayBuffer[String]()
    if(app1Cover>=0.5&&app2Cover>=0.5&&arr(3)!="1"&&arr(4)!="1"){res+=arr(0);res+=arr(1)}
    else if(app1Cover>=0.5&&app2Cover<0.5&&arr(3)!="1"){res+=arr(0)}
    else if(app1Cover<0.5&&app2Cover>=0.5&&arr(4)!="1"){res+=arr(1)}
    else if(app1Cover<0.5&&app2Cover<0.5){}
    res
  }).distinct(60).saveAsTextFile("blst")
  import java.sql.{Connection, DriverManager, ResultSet}

  /**
    * Created by linsong.chen on 2016/12/6.
    */
  object MysqlUtil {

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://l-db1-1.prod.cn2.corp.agrant.cn:3306/Adx?useUnicode=true&characterEncoding=UTF8"
    val username = "dmteam"
    val password = "2Ye7mDoJKaXbsbF8cIX4M13dMhvk0+utafLOZ65+O2Vd"

    def getConn(): Connection = {
      var connection:Connection = null
      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
      } catch {
        case e:Exception => println("wrong to get connection \n" +e.getMessage )
      }
      connection
    }
    def insertsql(sql: String) {
      var connection = getConn()
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          val resultSet = statement.executeUpdate(sql)
        } catch {
          case e:Exception => println(s"wrong to insert sql:$sql \n"+e.getMessage)
        }
      }
      connection.close()
    }
    def query(sql:String):ResultSet = {
      var connection = getConn()
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          val resultSet = statement.executeQuery(sql)
          return resultSet
        } catch {
          case e:Exception => println("wrong to query sql \n"+e.getMessage)
        }
      }
      connection.close()
      null
    }
    def update(sql:String,connection: Connection):Int = {
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          val resultSet = statement.executeUpdate(sql)
          return resultSet
        } catch {
          case e:Exception => println("wrong to query sql \n"+e.getMessage)
        }
      }
      1
    }
    def insertArraysqls(sqls:Array[String]){
      var connection = getConn()
      if(connection!=null){
        try {
          val statement = connection.createStatement()
          sqls.foreach(sql=>{
            statement.executeUpdate(sql)
          })
        } catch {
          case e:Exception => println("wrong to insert sql list \n"+e.getMessage)
        }
      }
      connection.close()
    }
  }
  val res = MysqlUtil.query("select count(*) as rowCount from Adx.AppBase where dateline like DATE_FORMAT(CURDATE(),'%Y%m%d') and (appname = '' or recentupdatetime not like CONCAT(DATE_FORMAT(NOW(),'%Y'),'%'))")
  res.next();
  val size  = res.getInt("rowCount")
  val appBlack = new Array[String](size)
  val resultSet = MysqlUtil.query("SELECT appid FROM Adx.AppBase where dateline like DATE_FORMAT(CURDATE(),'%Y%m%d') and (appname = '' or recentupdatetime not like CONCAT(DATE_FORMAT(NOW(),'%Y'),'%'))")
  val arrayBuffer = new ArrayBuffer[String]()
  val tempAppBlack = appBlack.map(x=>{
    resultSet.next()
    val appid = resultSet.getString("appid")
    appid
  })
  val appBlackRdd = sc.parallelize(tempAppBlack.toSeq)
  val appBlackRdd = sc.parallelize(tempAppBlack.toSeq)
  val calendar1 = Calendar.getInstance();
  val df1 = new SimpleDateFormat("yyyy-MM-dd-HH")
  val now = df1.format(calendar1.getTime())
  appBlackRdd.union(appBlackFromLog).filter(appid=>appid!="youku_app"&&appid!="netease_newsreader_android.apk"&&appid!="com.android.browser"&&appid.size>0).saveAsTextFile(s"/user/dm/spider/app/blacklist4bidder/appblack.$now")
}
