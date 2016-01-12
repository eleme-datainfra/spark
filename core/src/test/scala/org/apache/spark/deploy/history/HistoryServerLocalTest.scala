package org.apache.spark.deploy.history

import java.io.File

import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.{SecurityManager, SparkConf}


object HistoryServerLocalTest {

  private val logDir = new File("core/src/test/resources/spark-events")

  private var provider: FsHistoryProvider = null
  private var server: HistoryServer = null
  private var port: Int = -1

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

    ShutdownHookManager.addShutdownHook{ () => {
        println("Server stopping...")
        server.stop()
      }
    }

    println("Server started...")

    while(true) { Thread.sleep(Int.MaxValue) }
  }

}
