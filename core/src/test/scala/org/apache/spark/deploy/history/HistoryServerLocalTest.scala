package org.apache.spark.deploy.history

import java.io.File

import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf}

/**
 * Created by Wang Haihua on 2015/12/21.
 */
object HistoryServerLocalTest {

  //这个目录中含有一些预先放置的application
  private val logDir = new File("core/src/test/resources/spark-events")

  private var provider: FsHistoryProvider = null
  private var server: HistoryServer = null
  private var port: Int = -1

  /**
   * 在本地启动一个HistoryServer用来测试，初始化含有一些已有的app log
   * 不需要启动参数，所以便于测试
   *
   * 1. 测试从master的completed app list跳转到history server，可以直接读取到这个app的log
   *   1) 先启动本地的history Server
   *   2) 打开history server页面：http://localhost:18080/history
   *   3) 动态添加到 logDir 一个新的app event，例如app-event-test
   *   4) 打开对应event的页面：http://localhost:18080/history/app-event-test
   *   5) 可以看到这个app的event细节，而不是not-found
   *
   *
   *
   * @param args
   */
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", logDir.getAbsolutePath)
      .set("spark.history.fs.updateInterval", "0")
      .set("spark.testing", "true")
    provider = new FsHistoryProvider(conf)
    provider.checkForLogs()
    val securityManager = new SecurityManager(conf)

    server = new HistoryServer(conf, provider, securityManager, 18080)
    server.initialize()
    server.bind()
    port = server.boundPort

    Utils.addShutdownHook { () => {println("Server stopping..."); server.stop()} }

    println("Server started...")

    while(true) { Thread.sleep(Int.MaxValue) }
  }

}
