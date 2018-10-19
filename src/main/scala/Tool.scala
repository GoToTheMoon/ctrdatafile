import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import java.util.regex.Pattern

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer
//import org.apache.log4j.{Level, Logger}
//Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

object Tool extends Serializable {
  // 删除指定path文件
  def deleteExitFile(path: String): Boolean = {
    val HDFSStr = "hdfs://agrant"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFSStr), hadoopConf)
    val output = s"hdfs://agrant$path"
    val dfi = hdfs.exists(new org.apache.hadoop.fs.Path(output))
    val ifExit = hdfs.delete(new org.apache.hadoop.fs.Path(output), true)
    ifExit
  }

  // 删除指定文件夹下，like_str开头的文件
  def deleteExitFileLike(path: String, like_str: String): Boolean = {
    val HDFSStr = "hdfs://agrant"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFSStr), hadoopConf)
    val output = s"hdfs://agrant$path"
    val filepath = new org.apache.hadoop.fs.Path(output)
    val files = hdfs.listStatus(filepath).map(_.getPath.toString).filter(_.contains(like_str))
    var ifExit = false
    for (i <- files) {
      val ifExit = hdfs.delete(new org.apache.hadoop.fs.Path(i), true)
    }
    val result = if (hdfs.listStatus(filepath).map(_.getPath.toString).filter(_.contains(like_str)).size < files.size) true else false
    result
  }

  /**
    * 获取指定路径下的文件数
    * @param path
    * @return nums
    */
  def get_file_num(path: String): Int={
    val HDFSStr = "hdfs://agrant"
    val paths = if (path.endsWith("/")) path else s"$path/"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFSStr), hadoopConf)
    val filepath = new org.apache.hadoop.fs.Path(paths)
    val files = hdfs.listStatus(filepath)
    val nums = files.length
    nums
  }

  /**
    * 判断指定的路径是否存在
    * @param path
    * @return
    */
  def file_exists(path: String): Boolean={
    val HDFSStr = "hdfs://agrant"
    val paths = if (path.endsWith("/")) path else s"$path/"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFSStr), hadoopConf)
    val filepath = new org.apache.hadoop.fs.Path(paths)
    val filte_exists = hdfs.exists(filepath)
    filte_exists
  }
  /**
    * 评价DF模型准确率
    *
    * @param dic 数据类型如下
    *            |-- label: integer (nullable = true)
    *            |-- prediction: double (nullable = true)
    *            |-- sum(nums): long (nullable = true)
    */
  def evaluatin4DF(dic: DataFrame): Unit = {
    val a = dic.filter("label = 1 and prediction =1.0").rdd.take(1).map(_.get(2)).head.toString.toDouble
    val b = dic.filter("label = 1 and prediction =0.0").rdd.take(1).map(_.get(2)).head.toString.toDouble
    val c = dic.filter("label = 0 and prediction =1.0").rdd.take(1).map(_.get(2)).head.toString.toDouble
    val d = dic.filter("label = 0 and prediction =0.0").rdd.take(1).map(_.get(2)).head.toString.toDouble
    val accuracy = (a + d) / (a + b + c + d)
    val precision = a / (a + c) // 分类器判定的正例中真正的正例样本概率
    val recall = a / (a + b) // 正确判断的正例占总正例的概率
    val F1 = 2 * (precision * recall) / (precision + recall)
    println()
    println(s"TP-(1,1):\t $a ")
    println(s"FN-(1,0):\t $b ")
    println(s"FP-(0,1):\t $c ")
    println(s"TN-(0,0):\t $d ")
    println(s"accuracy:\t $accuracy ")
    println(s"precision:\t $precision ")
    println(s"recall:\t $recall ")
    println(s"F1:\t $F1")
    println()
  }

  /**
    * 计算Sensitivity
    */
  def get_fpr_tpr_4ROC(dic: DataFrame): Array[(Double, Double)] = {
    // TP
    val a = if (dic.filter("label = 1 and prediction =1.0").count != 0) dic.filter("label = 1 and prediction =1.0").rdd.take(1).map(_.get(2)).head.toString.toDouble else 0.0
    // FN
    val b = if (dic.filter("label = 1 and prediction =0.0").count != 0) dic.filter("label = 1 and prediction =0.0").rdd.take(1).map(_.get(2)).head.toString.toDouble else 0.0
    // FP
    val c = if (dic.filter("label = 0 and prediction =1.0").count != 0) dic.filter("label = 0 and prediction =1.0").rdd.take(1).map(_.get(2)).head.toString.toDouble else 0.0
    // TN
    val d = if (dic.filter("label = 0 and prediction =0.0").count != 0) dic.filter("label = 0 and prediction =0.0").rdd.take(1).map(_.get(2)).head.toString.toDouble else 0.0
    val false_positive_rate = c / (c + d) //FPR=FP/(FP+TN) 假正概率 将负例分为正例的概率
    val true_positive_rate = a / (a + b) //TPR=TP/(TP+FN) 真正概率 将正例分对的概率
    Array((false_positive_rate, true_positive_rate))
  }

  /**
    * 得到ROC曲线需要的对应阈值的Sensitivity
    *
    * @param threshold 阈值
    */
  def roc_evaluate(spark: SparkSession, predictions: DataFrame, threshold: Double): Array[(Double, Double)] = {
    predictions.createOrReplaceTempView("predictionss")
    val trans_prediction_sql = s"case when prediction < $threshold then 0.0 else 1.0 end as prediction"
    val dic = spark.sql(s"select label,$trans_prediction_sql,1 as nums from predictionss").groupBy("label", "prediction").agg("nums" -> "sum")
    val arr = get_fpr_tpr_4ROC(dic)
    arr
  }

  /**
    * 计算时间差，返回运行时间（几时几分几秒）
    *
    * @param time1 起始
    * @param time2 截止
    * @return
    */
  def get_time_period(time1: Date, time2: Date): String = {
    val time_period_hours = (time2.getTime - time1.getTime) / (1000 * 60 * 60)
    val time_period_mins = ((time2.getTime - time1.getTime) % (1000 * 60 * 60)) / (1000 * 60)
    val time_period_secs = ((time2.getTime - time1.getTime) % (1000 * 60)) / 1000
    s"$time_period_hours Hour $time_period_mins Min $time_period_secs Sec"
  }

  /**
    * 将device转换成MD5加密
    *
    * @param id
    * @return
    */
  def toMd5(id: String): String = {
    import java.nio.charset.Charset

    import com.google.common.hash.Hashing.md5
    md5.hashString(id.toUpperCase(), Charset.forName("utf-8")).toString().toLowerCase()
  }

  /**
    * APP端device_id逻辑：
    * 如果device_id长度为32，则表明為md5之后的；
    * 如果不是32位，则需要md5加密
    *
    * @param id
    * @return
    */
  def toMd5_full(id: String): String = {
    import java.nio.charset.Charset

    import com.google.common.hash.Hashing.md5
    val exchanged_id = md5.hashString(id.toUpperCase(), Charset.forName("utf-8")).toString().toLowerCase()
    val md5edid = if (id.isEmpty) "has_no_deviceid" else if (id.size == 32) id else exchanged_id
    md5edid
  }

  /**
    * 过滤有问题的deviceid以及其md5之后的id
    */
  def filter_wrong_devicied(deviceid: String): Boolean = {
    val wrong_devicied_str = "0000000000000000,00000000000000,B0000000,000000000000000,00000000-0000-0000-0000-000000000000,00000000,111111111111111,111111111111119,865620033222222,866146032222220,869953023333339,863339036444444,869401024444440,A1000055666666,867391036666662,869718027777777,558888888888888,861579037888888,888888888888889,88888888888888,888888888888888,868888888888888,866654036888888,123456789112345,012345678912345,812345678912345,123456789123456"
    val wrong_devicied_arr = wrong_devicied_str.split(",")
    val wrong_MD5edid_arr = wrong_devicied_arr.map(toMd5_full(_))
    val wrong_all = wrong_devicied_arr ++ wrong_MD5edid_arr
    wrong_all.contains(deviceid)
  }

  def nnHash(tag: String): Int = tag.hashCode & 0x7FFFFF

  /**
    * 对样本字段进行hash处理，生成索引
    */
  def hash_sample(deviceid: String, features: org.apache.spark.ml.linalg.Vector): Int = {
    val str = deviceid + "_" + features.toString
    val indexs = nnHash(str)
    indexs
  }

  /**
    * 将 probability 字段解释为概率
    */
  def explaine_probability(probability: org.apache.spark.ml.linalg.Vector): Double = {
    val probabilitys = probability
    val prediction = probability.toArray.mkString(",").split(",").last.split("]").head.toDouble
    f"$prediction%1.5f".toDouble
  }

  /**
    * 将 probability 字段解释为概率
    */
  def get_real_probability(probability: org.apache.spark.ml.linalg.DenseVector): Double = {
    var result = 0.0
    if (probability.size == 2) {
      val org.apache.spark.ml.linalg.DenseVector(arr) = probability
      val Array(pre_b, real_probability) = arr
      result = real_probability
    } else {
      result = 0.0
    }
    result
  }

  /**
    * 获取bidder时间字段的小时数
    * @param time
    * @return
    */
  def praseTime(time: String): String = {
    // val time= "2017-11-22 22:29:39"
    val hour = time.split(" ").tail(0).split(":")(0)
    hour
  }

  /**
    * 解析region字段到省一级
    *
    * @param sc
    * @param region
    * @return
    */
  def praseRegion(sc: SparkContext, region: String): String = {
    // val region = "25202"
    val region_mapping = sc.textFile("/user/dm/wenbin.lu/region-mapping").map(_.split(",")).map(x => (x(1), x(0))).cache
    val region_rdd = region_mapping.filter(_._1 == region)
    val province_label = if (region_rdd.count == 1) region_rdd.map(_._2).take(1).head else "00000"
    province_label
  }

  /**
    * 解析region字段到省一级
    *
    * @param region
    * @return
    */
  def praseRegion2(region: Int): String = {
    // val region = "25202"
    val province = region.toString.split("").take(2).mkString("")
    province
  }

  /**
    * 对Dataframe的column_name列进行one-hot编码
    *
    * @param df
    * @param column_name
    * @return
    */
  def onhot_index(df: DataFrame, column_name: String): DataFrame = {
    val index_column = s"${column_name}index"
    val vec_column = s"${column_name}Vec"
    val indexer = new StringIndexer().setInputCol(column_name).setOutputCol(index_column).fit(df)
    val indexed = indexer.transform(df)
    val encoder = new OneHotEncoder().setInputCol(index_column).setOutputCol(vec_column).setDropLast(false)
    val encoded = encoder.transform(indexed)
    encoded
  }


  /**
    * 对Dataframe的column_name列进行数值化处理
    *
    * @param df
    * @param column_name
    * @return
    */
  def indexer_index(df: DataFrame, column_name: String): DataFrame = {
    val index_column = s"${column_name}index"
    val indexer = new StringIndexer().setInputCol(column_name).setOutputCol(index_column).fit(df)
    val indexed = indexer.transform(df)
    indexed
  }

  /**
    * 将训练模型的label为String类型转变为Int
    *
    * @param positive_tag
    * @return
    */
  def praseString2Int(positive_tag: String): Int = {
    positive_tag.toInt
  }

  /**
    * 返回prediction对应的segment等级
    *
    * @param prediction
    * @return
    */
  def prase_predict_2_segment(prediction: Double): Int = {
    val predictionList = Array(.1, .2, .3, .4, .5, .6, .7, .8, .9, Int.MaxValue.toDouble)
    val gradeList = predictionList zip (1 to predictionList.size).reverse
    val grade = gradeList.filter(prediction < _._1).head._2
    grade
  }

  //返回最近n天的Array格式yyyy-mm-dd
  def getRecentNDays(n: Int): Array[String] = {
    var recentDayMap = for (i <- 1 to n) yield (LocalDateTime.now().minusDays(i).toString.take(10))
    recentDayMap.toArray
  }
  //返回最近n天的Array格式yyyymmdd
  def getRecentNDays_v2(n: Int): Array[String] = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    var recentDayMap = for (i <- 1 to n) yield (sdf1.format(sdf.parse(LocalDateTime.now().minusDays(i).toString.take(10)).getTime()))
    recentDayMap.toArray.reverse
  }
  //返回时间之间的小时，格式yyyyMMddHH
  def getRecentHour(start_hour: String, end_hour: String): Array[String] = {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd-HH")
    var start_hour_time = sdf1.parse(start_hour).getTime()
    val end_hour_time = sdf1.parse(end_hour).getTime()
    val hour_array = scala.collection.mutable.ArrayBuffer[String]()
    while(start_hour_time <= end_hour_time){
      hour_array.append(sdf1.format(start_hour_time))
      start_hour_time += 60 * 60 * 1000
    }
    hour_array.toArray
  }

  /**
    * 在HDFS目录下，获取path目录下，以filetype开头的z最新的N个文件
    *
    * @param N 获取最近的文件个数 !!! 注意，只能获取文件，不能获取文件夹
    * @return
    */
  def getRecentNFile(sc: SparkContext, path: String, filetype: String, N: Int): Array[String] = {
    val HDFSStr = "hdfs://agrant"
    val paths = if (path.endsWith("/")) path else s"$path/"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(HDFSStr), hadoopConf)
    val fileStr = s"${HDFSStr}$paths$filetype"
    val filepath = new org.apache.hadoop.fs.Path(paths)
    val files = hdfs.listStatus(filepath)
    files.length
    val feedNames = sc.parallelize(files.map(x => (x.getPath.toString, x.getModificationTime)).filter(_._1.startsWith(fileStr))).sortBy(_._2, false).take(N).map(_._1)
    feedNames
  }

  /**
    * 获取考拉APP用户的label 0:新访客；1:老客未购买；2:购买过
    */
  def getUserLabel4kaolaApp(pvRdd: RDD[String], partitions: Int): RDD[(String, (String, String))] = {
    val user_label = pvRdd.map(x => {
      val d = x.split("\t")
      val Array(deviceid, os, label) = Array(d(2), d(1), d.last)
      (deviceid, os, label)
    }).filter(x => {
      x._1 != "00000000-0000-0000-0000-000000000000" &&
        (x._1.length == 36 || x._1.length == 15 || x._1.length == 16) &&
        (x._2 == "iOS" || x._2 == "Android")
    }).map(x => {
      val agsid = toMd5(x._1)
      val os = x._2
      val label = if (x._3.length > 5 || x._3 == "null") "Unknown" else x._3
      (agsid, (os, label))
    }).repartition(partitions).distinct.groupByKey.map(x => {
      val (agsid, buffer) = (x._1, x._2.toList)
      val (os, label) = if (buffer.size == 1) buffer.last else buffer.sortBy(_._2).head
      (agsid, (os, label))
    })
    user_label
  }

  /**
    * 1. 对特征序列化
    */
  def get_feature(bidder_log: DataFrame, spark: SparkSession): DataFrame = {
    @transient
    val ssc = new SQLContext(spark.sparkContext)

    val samples_filledNa = bidder_log.na.fill(9.99)
    ssc.udf.register("praseTime", praseTime _)
    val feature_selected = samples_filledNa.selectExpr("md5edid", "positive_tag", "channel", "praseTime(time) as hour", "time", "ua", "region", "dt", "pf", "nt", "browser", "kernel", "dtbrand", "dtmodel")
    val df_ori = indexer_index(feature_selected, "channel")
    val df_a = indexer_index(df_ori, "hour")
    val df_b = indexer_index(df_a, "region")
    val df_g = indexer_index(df_b, "dt")
    val df_h = indexer_index(df_g, "pf")
    val df_j = indexer_index(df_h, "nt")
    val df_k = indexer_index(df_j, "browser")
    val df_l = indexer_index(df_k, "kernel")
    val df_m = indexer_index(df_l, "dtbrand")
    val indexed_feature = indexer_index(df_m, "dtmodel")
    val featuresArray = Array("channelindex", "hourindex",  "dtindex", "pfindex", "ntindex", "browserindex", "kernelindex", "dtbrandindex", "dtmodelindex")
    val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")
    val vecDF = assembler.transform(indexed_feature)
    val label_features = vecDF.select("md5edid", "positive_tag", "features", "channel", "hour", "province", "categoryid", "stype", "bl", "bt", "dt", "pf", "nt", "browser", "kernel", "dtbrand", "dtmodel")
    label_features
  }

  /**
    * 2. 对特征序列化并one-hot编码
    */
  def get_feature_onehot(bidder_log: DataFrame, spark: SparkSession): DataFrame = {
    @transient
    val ssc = new SQLContext(spark.sparkContext)

    val samples_filledNa = bidder_log.na.fill(9.99)
    ssc.udf.register("praseTime", praseTime _)
    val feature_selected = samples_filledNa.selectExpr("md5edid", "positive_tag", "channel", "praseTime(time) as hour", "ua", "region", "dt", "pf", "nt", "browser", "kernel", "dtbrand", "dtmodel")
    val df_ori = onhot_index(feature_selected, "channel")
    val df_a = onhot_index(df_ori, "hour")
    val df_b = onhot_index(df_a, "region")
    val df_g = onhot_index(df_b, "dt")
    val df_h = onhot_index(df_g, "pf")
    val df_j = onhot_index(df_h, "nt")
    val df_k = onhot_index(df_j, "browser")
    val df_l = onhot_index(df_k, "kernel")
    val df_m = onhot_index(df_l, "dtbrand")
    val on_hoted_feature = onhot_index(df_m, "dtmodel")
    val featuresArray = Array("channelVec", "hourVec","regionVec",  "dtVec", "pfVec", "ntVec", "browserVec", "kernelVec", "dtbrandVec", "dtmodelVec")
    val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")
    val vecDF = assembler.transform(on_hoted_feature)
    val label_features = vecDF.select("md5edid", "positive_tag", "features", "channel", "hour", "region", "dt", "pf", "nt", "browser", "kernel", "dtbrand", "dtmodel")
    label_features
  }

  def get_key(map: scala.collection.mutable.HashMap[org.apache.spark.ml.PipelineModel, Int], value: Int): org.apache.spark.ml.PipelineModel = {
    var key: org.apache.spark.ml.PipelineModel = null
    for (get_key <- map.keySet) {
      if (map.get(get_key).equals(Some(value))) {
        key = get_key
      }
    }
    key
  }
  /**
    * 获取dateframe中任意元素的长度
    */
  def get_length(x: Any): Int = {
    val result = if (x.isInstanceOf[String]) x.asInstanceOf[String].length else x.toString.length
    result
  }
  //对ua信息进一步的处理
  def fill_ua_freature_unknown_v2(feature: String, ua: String, index: Int): String = {
    val splitPatternAll = Pattern.compile("""/| |-|;|_|\(|\)|\[|\]|%|\+|%3B""")
    val arr = splitPatternAll.split(ua).filter(_ != "")
    val result = if (feature != "Unknown" && feature != "") {
      feature
    } else if ((ua.startsWith("Dalvik") || ua.startsWith("Mozilla")) && ua.split("""; U;|;|\)|%3B""").size > 2) {
      val Dalvik_arr = Array(ua.split(""" |%""").head.trim, ua.split("""; U;|;|\)|%3B""")(1).trim, ua.split("""; U;|;|\)|%3B""")(2).trim.split(" ").head, arr.head.trim)
      Dalvik_arr(index - 1)
    } else if (arr.length > index && index > 0) {
      arr(index - 1)
    } else {
      arr.head
    }
    result
  }

  /**
    *对数据打上标签
    */
  def label_tag(positive_tag: String):Int = {
    val label = if (positive_tag == "1" ) 1 else 0
    label  }
  def dealdatahour(time:String): Int={
    val t=time.split(' ')
    val h=t(1).split(':')(0).trim
    h.toInt
  }

  //将一列数据按照bin进行分箱
  def assisgn_group(x: Double, bin: List[Double]): Double={
    val N = bin.length
    val bound = if(x <= bin.min){
      bin.min
    }else if(x > bin.max){
      10e10
    }else{
      val media_bin = for(i <- List.range(0,N-1) if (bin(i) < x && x <= bin(i+1))) yield bin(i+1)
      media_bin.head
    }
    bound.toString.toDouble
  }

  /**
    * 卡方数据分箱
    * */
  def chimerge_maxinterval(spark: SparkSession,sc: SparkContext, data: DataFrame, colname: String, target: String, max_interval: Int, bad_rate: Double): List[Double] = {
    val over_rate = sc.broadcast(bad_rate)
    val data_ori = data.select(colname,target).filter(s"$colname >= 0 ").persist(StorageLevel.MEMORY_AND_DISK_SER)
    var col_levels = data_ori.select(colname).distinct.rdd.map(x=>{val Row(a)=x; a}).collect.toList.map(x => x.toString.toDouble).sorted
    val N_distinct = col_levels.length
    import spark.implicits._
    val ssc = new SQLContext(spark.sparkContext)
    ssc.udf.register("assisgn_group", assisgn_group _)
    val cutoff_points = if (N_distinct <= max_interval){
      println(s"类别数小于最大间隔数")
      col_levels
    }else{
      //如果类别数大于100的话，将其合并到100以内
      val data_ori_process = if (N_distinct > 100){
        val ind_x = for(i <- List.range(1, 100)) yield (i * N_distinct/ 100.0).toInt
        val split_x = for(i <- ind_x) yield col_levels(i).toString.toDouble
        val splite_x_sort = split_x.sorted
        data_ori.selectExpr(colname, target).map(x => {
          val col_num = x.get(0).toString.toDouble
          val label = x.getInt(1)
          val bin = assisgn_group(col_num, splite_x_sort)
          (col_num, label, bin)
        }).toDF(colname,target,"bin")
      }else{
        data_ori.selectExpr(colname, target, s"${colname} as bin")
      }
      val group_data = data_ori_process.map(x =>{
        val bin = x.getDouble(2)
        val bad_num  = if(x.getInt(1) == 0) 1 else 0
        val good_num = if(x.getInt(1) == 1) 1 else 0
        val total_num = 1
        (bin, bad_num, good_num, total_num)
      }).groupBy("_1").agg("_2"->"sum","_3"->"sum","_4"->"sum").toDF("col_bin","bad_num","goog_num", "total_num").persist(StorageLevel.MEMORY_AND_DISK_SER)
      col_levels = group_data.select("col_bin").rdd.map(x=>{val Row(a)=x; a}).collect.toList.map(_.toString.toDouble).sorted
      //最开始，将每一条数据作为分箱的边界
      val group_intervals = new ListBuffer[ListBuffer[Double]]
      for(i <- col_levels){
        val a =  ListBuffer(i)
        group_intervals += a
      }
      var group_num = group_intervals.length
      while (group_intervals.length > max_interval ){
        val chisq_list = new ListBuffer[Double]
        for (interval <- group_intervals){
          val df2 = group_data.filter(x=>interval.contains(x.getDouble(0)))
          //计算这一类的卡方值
          val chisq = df2.map(x=>{
            val expected = x.getLong(3) * over_rate.value //每一组的样本数乘上bad占比
            val chi = ( x.getLong(1) - expected ) * ( x.getLong(1) - expected )/expected
            (chi)
          }).toDF("chi").agg("chi"->"sum").rdd.map(x=>{val Row(a)=x;a}).collect.head.toString.toDouble
          chisq_list += chisq
        }
        //获取到最小卡方值的索引位置以及下一步要合并的索引位置
        val min_position = chisq_list.zipWithIndex.min._2
        val combined_position = if (min_position == 0) {
          1
        }else if(min_position == group_num - 1) {
          min_position - 1
        }else{
          if (chisq_list(min_position - 1) <= chisq_list(min_position + 1)){
            min_position - 1
          }else{
            min_position + 1
          }
        }
        //合并分隔，并删除掉原始的数据
        group_intervals(min_position) = group_intervals(min_position) ++ group_intervals(combined_position)
        group_intervals.remove(combined_position)
        group_num = group_intervals.length
      }
      val group_intervals_sort = for( i <- group_intervals) yield i.sorted
      group_data.unpersist()
      group_intervals_sort.map(_.last).toList
    }
    data_ori.unpersist()
    cutoff_points
  }

  /**
    *
    *对特征进行哈希处理
    */
  def feature_hash(feature: String, hash_space: Int): String={
    val feature_hashnum = (Math.abs(feature.hashCode()) % hash_space).toString
    feature_hashnum
  }

  /**
    * 对vector的数据进行处理
    * @param v
    * @return 返回逗号分割的字符串
    */
  def deal_vector(v: org.apache.spark.ml.linalg.Vector): String={
    val sv = v.toString()
    val splitPatternAll = Pattern.compile(""",|\[|\]|\(|\)""")
    val arr = splitPatternAll.split(sv).filter(_!="")
    val list_temp = scala.collection.mutable.ListBuffer[Int]()
    val lenght = arr(0).toInt
    val index_1 = arr(1).toInt
    for(i <- 1 to lenght){
      list_temp += 0
    }
    list_temp(index_1) = 1
    list_temp.mkString(",")
  }
  //手动进行one-hot
  def deal_onehot_hand(feature: String, feature_data: Any): String = {
    val stype_map = Map("1"->"1,0,0,0,0,0,0", "3"->"0,1,0,0,0,0,0", "6"->"0,0,1,0,0,0,0", "7"->"0,0,0,1,0,0,0", "11"->"0,0,0,0,1,0,0", "13"->"0,0,0,0,0,1,0")
    val position_map = Map("0" -> "1,0,0,0", "1" -> "0,1,0,0", "2" -> "0,0,1,0")
    val dt_map = Map("T" -> "1,0,0", "P" -> "0,1,0")
    val pf_map = Map("A" -> "1,0,0,0", "W" -> "0,1,0,0", "I" -> "0,0,1,0")
    val bklevel_map = Map("0" -> "1,0,0,0", "3" -> "0,1,0,0", "4" -> "0,0,1,0")
    val pt_map = Map("201" -> "1,0,0,0,0,0","202" -> "0,1,0,0,0,0","203" -> "0,0,1,0,0,0", "401" -> "0,0,0,1,0,0","402" -> "0,0,0,0,1,0")
    val plantype_map = Map("200" -> "1,0,0","400" -> "0,1,0")
    val grouptype_map = Map("201" -> "1,0,0,0,0,0","202" -> "0,1,0,0,0,0","203" -> "0,0,1,0,0,0", "401" -> "0,0,0,1,0,0","402" -> "0,0,0,0,1,0")
    val nt_map = Map("wifi" ->"1,0,0,0", "4G" -> "0,1,0,0", "ethernet" -> "0,0,1,0", "Unknown" -> "0,0,0,1")
    val kernel_map = Map("Webkit" -> "1,0,0,0,0,0", "Presto" -> "0,1,0,0,0,0", "Gecko" -> "0,0,1,0,0,0", "Trident" -> "0,0,0,1,0,0", "Blink" -> "0,0,0,0,1,0", "Unknown" -> "0,0,0,0,0,1")
    val channel_map = Map("GDTAPP" -> "1,0,0,0,0,0,0,0,0", "BAIDUAPP"->"0,1,0,0,0,0,0,0,0", "ZCM"->"0,0,1,0,0,0,0,0,0","ADVIEW"->"0,0,0,1,0,0,0,0,0", "TANXAPP"->"0,0,0,0,1,0,0,0,0", "BAIDUFEED"->"0,0,0,0,0,1,0,0,0", "WINAAPP"->"0,0,0,0,0,0,1,0,0", "XTX"->"0,0,0,0,0,0,0,1,0")
    val tp_map = Map("1" -> "1,0,0,0,0,0,0", "3" -> "0,1,0,0,0,0,0", "6" -> "0,0,1,0,0,0,0", "7" -> "0,0,0,1,0,0,0", "11" -> "0,0,0,0,1,0,0", "13" -> "0,0,0,0,0,1,0")
    if(feature_data != null && feature_data != ""){
      feature match {
        case "stype" => stype_map.getOrElse(feature_data.toString, "0,0,0,0,0,0,1")
        case "position" => position_map.getOrElse(feature_data.toString, "0,0,0,1")
        case "dt" => dt_map.getOrElse(feature_data.toString, "0,0,1")
        case "pf" => pf_map.getOrElse(feature_data.toString, "0,0,0,1")
        case "bklevel" => bklevel_map.getOrElse(feature_data.toString, "0,0,0,1")
        case "pt" => pt_map.getOrElse(feature_data.toString, "0,0,0,0,0,1")
        case "plantype" => plantype_map.getOrElse(feature_data.toString, "0,0,1")
        case "grouptype" => grouptype_map.getOrElse(feature_data.toString, "0,0,0,0,0,1")
        case "nt" => nt_map.getOrElse(feature_data.toString, "0,0,0,1")
        case "kernel" => kernel_map.getOrElse(feature_data.toString, "0,0,0,0,0,1")
        case "channel" => channel_map.getOrElse(feature_data.toString, "0,0,0,0,0,0,0,0,1")
        case "tp" => tp_map.getOrElse(feature_data.toString, "0,0,0,0,0,0,1")
      }
    }else{
      feature match {
        case "stype" => "0,0,0,0,0,0,1"
        case "position" =>  "0,0,0,1"
        case "dt" => "0,0,1"
        case "pf" => "0,0,0,1"
        case "bklevel" => "0,0,0,1"
        case "pt" => "0,0,0,0,0,1"
        case "plantype" => "0,0,1"
        case "grouptype" => "0,0,0,0,0,1"
        case "nt" => "0,0,0,1"
        case "kernel" => "0,0,0,0,0,1"
        case "channel" => "0,0,0,0,0,0,0,0,1"
        case "tp" => "0,0,0,0,0,0,1"
      }
    }
  }
  //处理null和""的数据
  def deal_string_na(feature: String): String={
    val data = if(feature == null || feature == "") "-1" else feature
    data
  }

}