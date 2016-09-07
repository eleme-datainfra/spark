/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.estimator

import java.io.File

import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.scheduler._
import org.apache.spark.util.Utils

import scala.collection.mutable.ListBuffer


abstract class Estimator(conf: SparkConf, liveListenerBus: LiveListenerBus) {
  def estimate(event: SparkListenerEvent): Unit
}

class EstimatorListener(conf: SparkConf, liveListenerBus: LiveListenerBus) extends SparkListener {
  private val estimators = new ListBuffer[Estimator]()

  def init(): Unit = {
    getEstimators().foreach { estimator =>
      val constructor = estimator.getDeclaredConstructor(classOf[SparkConf],
        classOf[LiveListenerBus])
      constructor.setAccessible(true)
      estimators += constructor.newInstance(conf).asInstanceOf[Estimator]
    }
  }

  def getEstimators(): Seq[Class[_]] = {
    val packageName = this.getClass.getPackage().getName()
    val path = packageName.replace('.', '/')
    val classloader = Thread.currentThread().getContextClassLoader()
    val url = classloader.getResource(path)
    val dir = new File(url.getFile())
    val classes = new ListBuffer[Class[_]]()

    if (!dir.exists()) {
      return classes
    }

    val estimatorClass = classOf[Estimator]

    dir.listFiles().foreach { f =>
      if (f.isDirectory()) {
        classes.addAll(getClasses(f, packageName + "." + f.getName()))
      }
      val name = f.getName()
      if (name.endsWith(".class")) {
        val cls = Utils.classForName(packageName + "." + name.substring(0, name.length() - 6))
        if (cls.isAssignableFrom(estimatorClass) && !cls.equals(estimatorClass)) {
          classes.add(cls)
        }
      }
    }

    return classes
  }

  def handleEvents(event: SparkListenerEvent): Unit = {
    try {
      estimators.foreach(_.estimate(event))
    } catch {
      case e: Exception =>
        // do nothing
    }
  }

  /**
    * Called when a stage completes successfully or fails, with information on the completed stage.
    */
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    handleEvents(event)
  }

  /**
    * Called when a stage is submitted
    */
  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    handleEvents(event)
  }

  /**
    * Called when a task starts
    */
  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    handleEvents(event)
  }

  /**
    * Called when a task begins remotely fetching its result (will not be called for tasks that do
    * not need to fetch the result remotely).
    */
  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = {
    handleEvents(event)
  }

  /**
    * Called when a task ends
    */
  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    handleEvents(event)
  }

  /**
    * Called when a job starts
    */
  override def onJobStart(event: SparkListenerJobStart): Unit = {
    handleEvents(event)
  }

  /**
    * Called when a job ends
    */
  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    handleEvents(event)
  }

  /**
    * Called when environment properties have been updated
    */
  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    handleEvents(event)
  }
  /**
    * Called when a new block manager has joined
    */
  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    handleEvents(event)
  }

  /**
    * Called when an existing block manager has been removed
    */
  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved)
      : Unit = {
    handleEvents(event)
  }
  /**
    * Called when an RDD is manually unpersisted by the application
    */
  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    handleEvents(event)
  }

  /**
    * Called when the application starts
    */
  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    handleEvents(event)
  }

  /**
    * Called when the application ends
    */
  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    handleEvents(event)
  }

  /**
    * Called when the driver receives task metrics from an executor in a heartbeat.
    */
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    handleEvents(event)
  }

  /**
    * Called when the driver registers a new executor.
    */
  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    handleEvents(event)
  }

  /**
    * Called when the driver removes an executor.
    */
  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    handleEvents(event)
  }

  /**
    * Called when the driver receives a block update info.
    */
  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    handleEvents(event)
  }

  /**
    * Called when other events like SQL-specific events are posted.
    */
  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    handleEvents(event)
  }
}