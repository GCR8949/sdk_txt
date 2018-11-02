package com.mingrun.sdk.task

import java.sql.{Connection, PreparedStatement, SQLException}

import com.mingrun.sdk.function.FieldsFunc._
import com.mingrun.sdk.function.JDBCFunc._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by GCR8949 on 2018/10/29.
  */
object L_I_CLASS_40_640_1280_INDOOR {
  //  log最短字段要求
  val fieldsMinSize = 100

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("L_I_CLASS_40_640_1280_INDOOR").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val dataPath = ""
    val dataRDD = sc.textFile(dataPath)
    val accum = new LocationAccumulator
    sc.register(accum, "locationAccum")

    val dataByXYIDRDD = dataRDD.map(line => {
      var resStr: (String, String) = null

      if (line != null && ! line.equals("")) {
        val items = line.split(",", -1)

        if (items.length >= fieldsMinSize) {
          val lng = lngLatStr2Double(items(10))
          val lat = lngLatStr2Double(items(9))

          if (lng != 0.0 && lat != 0.0) {
            val xyID_40 = getXYID(lng, lat, 40)

            var locationRight = true
            val xyID_640 = getXYID(lng, lat, 640)
            val xyID_1280 = getXYID(lng, lat, 1280)

            var province = lengthCorrection(fieldNull2Unknown(items(14)), 100)
            var city = lengthCorrection(fieldNull2Unknown(items(15)), 100)

            /*
              针对有些log中没有省市的情况，使用其经纬度来重新获取
              但如果遇到网络问题等，在尝试获取默认次数仍然失败时
              则省市为“UNKNOWN”
              ----------------------------------------------------------
              已更新：
              鉴于没有省市的数据为少数，且通过其经纬度查询地址需要走网络
              这其中就加大了不少的资源消耗，降低了效率
              目前采用40栅格（40m * 40m）匹配已知省市做缓存，对于相同栅格内
              没有省市的情况做匹配处理，秉承着大数据下忽略个例的方针
              未匹配到的暂时舍弃
              ----------------------------------------------------------
              后面打算将每次的已知匹配表做存储并持续更新
              以达到尽可能匹配到所有已知区域
               */
            if ("UNKNOWN".equals(province) || "UNKNOWN".equals(city)) {
              val proAndcity = accum.value.getOrElse(xyID_40, null)

              if (proAndcity != null) {
                val location = proAndcity.split(",", -1)
                province = location(0)
                city = location(1)
              } else {
                //  未匹配到location，暂时舍弃
                locationRight = false
              }
            } else {
              accum.add(xyID_40 + "|" + province + "," + city)
            }

            if (locationRight) {
              val dateTime = timestamp2Date(items(19))
              val operator = lengthCorrection(fieldNull2Unknown(items(29)), 100)
              val netType = items(31)
              val rsrp = rsrpSinrStr2Int(items(35))
              val sinr = rsrpSinrStr2Int(items(36))

              //  如果rsrp或sinr不合规，后续不参与网络情况的计算和统计
              var netLevel = "0"

              if (rsrp < 1000 && sinr < 1000) {
                if (rsrp >= -100 && sinr >= 3) netLevel = "G"
                if (rsrp >= -100 && sinr <= 0) netLevel = "B1"
                if (rsrp <= -110 && sinr <= 0) netLevel = "B2"
              }

              val key = dateTime + "," + province + "," + city + "," + operator + "," + xyID_1280 + "," + xyID_640 + "," + xyID_40
              val value = netType + "," + netLevel

              resStr = (key , value)
            } else {
              //  未匹配到location，暂时舍弃
            }
          } else {
            //  作为必要字段的经纬度存在不合规的情况
          }
        } else {
          //  log字段小于最短字段要求
        }
      } else {
        //  log不合规
      }

      resStr
    }).filter(_ != null)


    val resRDD = dataByXYIDRDD.groupByKey().mapValues(lines => {
      var totalSmpl = 0L
      var lteSmpl = 0L
      var outlineSmpl = 0L
      var G = 0L
      var B1 = 0L
      var B2 = 0L

      val list = lines.toList

      list.foreach(line => {
        val netTypeAndLevel = line.split(",", -1)

        val netType = netTypeAndLevel(0)
        val netLevel = netTypeAndLevel(1)

        if ("LTE".equals(netType)) {
          lteSmpl = lteSmpl + 1L
        } else if ("".equals(netType)) {
          outlineSmpl = outlineSmpl + 1L
        }

        if ("G".equals(netLevel)) {
          G = G + 1L
        } else if ("B1".equals(netLevel)) {
          B1 = B1 + 1L
        } else if ("B2".equals(netLevel)) {
          B2 = B2 + 1L
        }

        totalSmpl = totalSmpl + 1L
      })

      totalSmpl + "," + lteSmpl + "," + outlineSmpl + "," + G + "," + B1 + "," + B2
    }).map(x => x._1 + "," + x._2)

    resRDD.foreachPartition(lines => {
      var connection: Connection = null
      var pstmt: PreparedStatement = null
      val sql = "insert into L_I_40_640_1280_INDOOR_TEST values(to_date(?,'yyyy/mm/dd '),?,?,?,?,?,?,?,?,?,?,?,?)"

      try {
        connection = getConnection()
        connection.setAutoCommit(false)
        pstmt = connection.prepareStatement(sql)

        lines.foreach(line => {
          val params = line.split(",", -1)

          if (params.length == 13) {
            for (i <- 0 until params.length) {
              pstmt.setObject(i + 1, params(i))
            }

            pstmt.addBatch()
          }
        })

        pstmt.executeBatch()
        connection.commit()
      } catch {
        case e: SQLException =>
          if (connection != null) connection.rollback()
          throw e
      } finally {
        if (pstmt != null) pstmt.close()
        if (connection != null) connection.close()
      }
    })

//    resRDD.map(x => x._1 + "," + x._2).coalesce(1).saveAsTextFile("")

    sc.parallelize(accum.value.toList).map(x => x._1 + "," + x._2).coalesce(1).saveAsTextFile("")

  }



}
