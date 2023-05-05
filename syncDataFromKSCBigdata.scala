package com.kingsoft.dc.offlinedev

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 大数据平台数据同步插件
 * 基于金山云大数据平台
 * author: liguangbiao
 * 2023.5.4
 * @param id
 * @param data
 */
case class Data(id: String,data: String)
class RunJar extends Serializable {

  def exec(args: Array[String]): Unit = {

    println("========== 用户程序开始执行 ===============")
    val api = new ApiEngine(args)
    val config = api.getArguments()
    val spark = api.getSparkSession()

    val executor1 = api.getDataSource1 //获取第一个数据源: 源
    val executor2 = api.getDataSource2 //获取第一个数据源: 目标

    println("")

    val plugin_param = JSON.parseObject(config("plugin_param"))
    val srcType = plugin_param.getJSONObject("config").getJSONArray("datasource")
      .getJSONObject(0).getJSONObject("auth")
      .getJSONObject("params").getString("ds_type")
    var dstType = plugin_param.getJSONObject("config").getJSONArray("datasource")
      .getJSONObject(1).getJSONObject("auth")
      .getJSONObject("params").getString("ds_type")
    println(s" ==plugin参数：${plugin_param.toString()} ")

    val doris_url = config("DORIS_FE_NODES")
    val doris_username = config("DORIS_USERNAME")
    val doris_password = config("DORIS_PASSWORD")
    val src_db_table_name = config("SRC_DB_TABLE_NAME")
    val dst_db_table_name = config("DST_DB_TABLE_NAME")

    // 拆分库表名称
    //val Array(srcDb, srcTable) = src_db_table_name.split("\\.")
    var Array(srcDb, srcTable) = Array("", "")
    if (src_db_table_name.contains(".")) {
      val Array(_Db, _Table) = src_db_table_name.split("\\.")
      srcDb = _Db
      srcTable = _Table
    }
    else {
      srcTable = src_db_table_name
    }
    //val Array(dstDb, dstTable) = dst_db_table_name.split("\\.")
    var Array(dstDb, dstTable) = Array("", "")
    if (dst_db_table_name.contains(".")) {
      val Array(_Db, _Table) = dst_db_table_name.split("\\.")
      dstDb = _Db
      dstTable = _Table
    }
    else {
      dstTable = dst_db_table_name
    }

    println(s" 来源数据源类型：$srcType 数据库：$srcDb 表：$srcTable,  目标类型：$dstType 库：$dstDb 表：$dstTable ")

    var source_url, source_username, source_password, source_table_name, target_url, target_username, target_password, target_table_name = ""
    val src_tbl_view = s"source_view_${srcTable}"
    val tgt_tbl_view = s"target_view_${dstTable}"
    // 查询SQL TODO:待调整支持配置指定
    var querySql = s"SELECT * FROM ${src_tbl_view}"
    // 写入SQL TODO:待调整支持配置指定
    val insertSql = s"INSERT INTO ${tgt_tbl_view} SELECT * FROM ${src_tbl_view}"

    var appName = "[" + srcType + "=>" + dstType + "]数据同步-" + src_db_table_name
    println(s" AppName：$appName")

    // 读取数据
    printf(" [%s] 开始加载数据 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
    var df = spark.emptyDataFrame
    // ===== 来源数据源 ====
    srcType match {
      case "hive" => {
        println("【Hive】")
        df = executor1.readDfData(srcDb, srcTable)
        println(s" 来源数据源类型 $srcType 数据量：${df.count()}")
        df.createOrReplaceTempView(src_tbl_view)
      }
      case "mysql" => {
        println("【MySQL】")
        df = executor1.readDfData(srcDb, srcTable)
        println(s" 来源数据源类型 $srcType 数据量：${df.count()}")
        df.createOrReplaceTempView(src_tbl_view)
      }
      case "elasticsearch" => {
        println(s"从 $dstType 同步数据,暂不支持")
        df.createOrReplaceTempView(src_tbl_view)
      }
      case "phoenix" => {
        println("来源数据源【doris】")
        source_url = doris_url
        source_username = doris_username
        source_password = doris_password
        source_table_name = src_db_table_name

        printf(" [%s] 创建来源数据临时视图 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
        val sourceView =
          s"""
             |CREATE TEMPORARY VIEW $src_tbl_view
             |USING doris
             |OPTIONS(
             |  "table.identifier"="$src_db_table_name",
             |  "fenodes"="$doris_url",
             |  "user"="$doris_username",
             |  "password"="$doris_password"
             |)
             |""".stripMargin
        spark.sql(sourceView)
        df = spark.sql(querySql)
        println(s" >>> 数据量：${df.count()}")
      }
      case _ => {
        println(srcType)
        println(s" 来源数据源类型 $srcType : 待实现")
        return
      }
    }
    df.show(5)

    // ===== 目标数据源 ====
    var result = spark.emptyDataFrame
    //def matchDstType(dstType : Any) = dstType match {
    dstType match {
      //case RealDsType.HIVE => {
      case "hive" => {
        printf(s" [%s] 开始执行 $dstType 数据写入 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
        executor2.writeDfData(df, dstDb, dstTable, SaveMode.Overwrite)
      }
      case "mysql" => {
        println("目标数据源【MySQL】")
        printf(s" [%s] 开始执行 $dstType 数据写入 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
        executor2.writeDfData(df, dstDb, dstTable, SaveMode.Overwrite)
      }
      case "phoenix" => {
        println("->doris.")
        dstType = "doris"
        target_url = doris_url
        target_username = doris_username
        target_password = doris_password
        target_table_name = dst_db_table_name
        //创建临时数据视图
        printf(" [%s] 创建目标临时视图 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
        val tagetView =
          s"""
             |CREATE TEMPORARY VIEW ${tgt_tbl_view}
             |USING doris
             |OPTIONS(
             |  "table.identifier"="$dst_db_table_name",
             |  "fenodes"="$doris_url",
             |  "user"="$target_username",
             |  "password"="$target_password"
             |)
             |""".stripMargin
        spark.sql(tagetView)
        printf(s" [%s] 开始执行 $dstType 数据写入 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
        result = spark.sql(insertSql)
      }
      case "elasticsearch" => {
        println(s" 目标数据源类型 $dstType : 待实现 ")
        return
      }
      case _ => {
        println(dstType)
        println(s" 目标数据源类型 $dstType : 可使用Spark数据同步组件实现同步")
        return
      }
    }
    spark.close()

    // ==== 开始数据同步 ====
//    if ("hive" != dstType ) {  // Hive 已通过excutor执行，MySQL/Doris使用connector写入
//      printf(s" [%s] 开始执行 $dstType 数据写入 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
//      result = spark.sql(insertSql)
//    }
    printf(" [%s] 写入完成，开始收集执行信息 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
    try {
      val rowIds = result.selectExpr("concat_ws(',', collect_list(_shard_id), collect_list(_partition_id), collect_list(_bucket_id)) as rowids").collect()
      printf("== 写入记录数：%d  返回信息: %s  \n", rowIds(0).getAs[String]("rowids").split(",").length, result.toJSON)
    } catch {
      case ex: Throwable => println(" 分析异常：" + ex)
    }

    printf(" [%s] 数据同步结束 \r\n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))

  }
}
