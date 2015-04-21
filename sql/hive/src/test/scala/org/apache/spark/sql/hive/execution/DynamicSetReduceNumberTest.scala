package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.log4j.{Level, Logger}

/**
 * Created by Administrator on 15-4-20.
 */
class DynamicSetReduceNumberTest extends PlanTest {

  test("test dynamic set reduce number") {
    val sql =
      """
        |select
        |concat(year,month,day) as stat_date,
        |param['phone'] as phone,
        |count(1) as pv
        |from ods.ods_log_nginx
        |where concat(year,month,day)=20140207
        |group by concat(year,month,day),param['phone']
      """.stripMargin

    Logger.getRootLogger.setLevel(Level.INFO)

    val sparkConf = new SparkConf()
      .setAppName("testReduceNumber")
      .setMaster("local[2]")
      .set("spark.reduce.autoPartition","true")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql(sql).collect()
  }
}
