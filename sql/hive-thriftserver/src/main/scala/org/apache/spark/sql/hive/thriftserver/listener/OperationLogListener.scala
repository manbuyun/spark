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
package org.apache.spark.sql.hive.thriftserver.listener

import java.math.RoundingMode
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.math.IntMath
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hadoop.hive.ql.session.OperationLog.LoggingLevel.EXECUTION

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.hive.thriftserver.ui._

/**
 * @author jinhai
 * @date 2020/07/27
 */
private[thriftserver] class OperationLogListener extends SparkListener with Logging {

  private val statementMap = new ConcurrentHashMap[String, OperationInfo]()
  private val jobMap = new ConcurrentHashMap[Int, OperationInfo]()
  private val stageMap = new ConcurrentHashMap[Int, OperationInfo]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // SPARK_JOB_GROUP_ID会被其他组件污染，比如BroadcastExchangeExec
    val statementId = jobStart.properties.getProperty(SparkContext.SPARK_JOB_STATEMENT_ID)
    val operationLog = statementMap.get(statementId).operationLog

    jobMap.put(jobStart.jobId, OperationInfo(operationLog, jobStart.time))

    val sb = new StringBuilder()
    sb.append(s"SparkContext: Starting job = ${jobStart.jobId}, ")
    sb.append(s"total stages = ${jobStart.stageIds.length}\n")

    operationLog.writeOperationLog(EXECUTION, sb.toString())
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val operationInfo = jobMap.remove(jobEnd.jobId)
    val duration = TimeUnit.MILLISECONDS.toSeconds(jobEnd.time - operationInfo.startTime)
    val logString = s"SparkContext: Ended job = ${jobEnd.jobId}, time taken $duration seconds\n"

    operationInfo.operationLog.writeOperationLog(EXECUTION, logString)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val statementId = stageSubmitted.properties.getProperty(SparkContext.SPARK_JOB_STATEMENT_ID)
    val operationLog = statementMap.get(statementId).operationLog

    val info = stageSubmitted.stageInfo
    stageMap.put(info.stageId, OperationInfo(operationLog, 0, info.numTasks))

    val stageIdStr = s"${info.stageId}.${info.attemptNumber()}"
    val logString = s"DAGScheduler: Submitting ${info.numTasks} tasks from stage-$stageIdStr\n"

    operationLog.writeOperationLog(EXECUTION, logString)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val operationInfo = stageMap.remove(info.stageId)

    val completed = operationInfo.current
    val stageIdStr = s"${info.stageId}.${info.attemptNumber()}"
    val logString = s"DAGScheduler: Completed $completed tasks from stage-$stageIdStr\n"

    operationInfo.operationLog.writeOperationLog(EXECUTION, logString)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val info = taskEnd.taskInfo
    val operationInfo = stageMap.get(taskEnd.stageId)

    // 当Cancel任务时，是异步runInBackground，会导致onStageCompleted先执行
    if (operationInfo == null) return
    operationInfo.current.incrementAndGet()

    if (isLogEnable(operationInfo.current.get(), operationInfo.total)) {
      val sb = new StringBuilder()
      sb.append(s"TaskSetManager: Finished task ${info.id} ")
      sb.append(s"in stage-${taskEnd.stageId}.${taskEnd.stageAttemptId} ")
      sb.append(s"in ${info.duration} ms on ${info.host} (executor ${info.executorId}) ")
      sb.append(s"(${operationInfo.current}/${operationInfo.total})\n")

      operationInfo.operationLog.writeOperationLog(EXECUTION, sb.toString())
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerThriftServerOperationStart => onOperationStart(e)
      case e: SparkListenerThriftServerOperationFinish => onOperationFinish(e)
      case e: SparkListenerThriftServerOperationClosed => onOperationClosed(e)
      case _ => // Ignore
    }
  }

  private def onOperationStart(e: SparkListenerThriftServerOperationStart): Unit = {
    statementMap.put(e.id, OperationInfo(e.operationLog, e.startTime))

    val sb = new StringBuilder()
    sb.append(s"SparkExecuteStatementOperation: Submitting query at ${e.startTime}\n")
    sb.append(s"SparkExecuteStatementOperation: Query ID = ${e.id}. User = ${e.userName}\n")

    e.operationLog.writeOperationLog(EXECUTION, sb.toString())
  }

  private def onOperationFinish(e: SparkListenerThriftServerOperationFinish): Unit = {
    val operationLog = statementMap.get(e.id).operationLog
    val duration = TimeUnit.MILLISECONDS.toSeconds(e.finishTime - statementMap.get(e.id).startTime)

    val sb = new StringBuilder()
    sb.append(s"SparkExecuteStatementOperation: Finished query with ${e.id}\n")
    sb.append(s"SparkExecuteStatementOperation: Total time taken $duration seconds\n")

    operationLog.writeOperationLog(EXECUTION, sb.toString())
  }

  private def onOperationClosed(e: SparkListenerThriftServerOperationClosed): Unit = {
    statementMap.remove(e.id)
  }

  private def isLogEnable(current: Int, total: Int): Boolean = {
    val divide = IntMath.divide(total, 20, RoundingMode.UP)
    if (current % divide == 0 || current == 1 || current == total) true else false
  }
}

private[thriftserver] case class OperationInfo(
    operationLog: OperationLog,
    startTime: Long = 0,
    total: Int = 0,
    var current: AtomicInteger = new AtomicInteger(0))
