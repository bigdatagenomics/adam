/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.instrumentation

import com.netflix.servo.monitor.{ Gauge, Monitor, MonitorConfig, CompositeMonitor }
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import com.netflix.servo.tag.Tags.newTag
import com.netflix.servo.tag.Tag
import org.bdgenomics.adam.instrumentation.ServoTimer._
import java.util.concurrent.TimeUnit._

/**
 * Timer that implements the [[com.netflix.servo.monitor.CompositeMonitor]] interface.
 * Sub-monitors contain specific metrics, and can be identified using the following tags:
 * - Total Time: [[ServoTimer.TotalTimeTag]]
 * - Count: [[ServoTimer.CountTag]]
 * - Mean: [[ServoTimer.MeanTag]]
 * - Max: [[ServoTimer.MaxTag]]
 * - Min: [[ServoTimer.MinTag]]
 * All timings are returned in nanoseconds.
 *
 * @param config the config for this timer; this config is propagated to sub-monitors
 */
class ServoTimer(config: MonitorConfig) extends CompositeMonitor[Object] with Taggable {

  private var baseConfig: MonitorConfig =
    config.withAdditionalTag(newTag(TimeUnitTagKey, NANOSECONDS.name())).
      withAdditionalTag(newTag(NameTagKey, config.getName))

  private val totalTimeNanos = new AtomicLong()
  private val count = new AtomicLong()

  private val totalTimeMonitor = new LongValueStatMonitor(withTag(TotalTimeTag), () => totalTimeNanos.longValue())
  private val countMonitor = new LongValueStatMonitor(withTag(CountTag), () => count.longValue())
  private val meanMonitor = new LongValueStatMonitor(withTag(MeanTag), () => {
    if (count.longValue() > 0) {
      (totalTimeNanos.doubleValue() / count.doubleValue()).toLong
    } else {
      0
    }
  })
  private val minMonitor = new ConditionalGauge(withTag(MinTag), (existingValue, newValue) => { newValue < existingValue })
  private val maxMonitor = new ConditionalGauge(withTag(MaxTag), (existingValue, newValue) => { newValue > existingValue })

  private val subMonitors = Seq(totalTimeMonitor, countMonitor, meanMonitor, minMonitor, maxMonitor)

  /**
   * Records an occurrence of the specified duration, in milliseconds
   */
  def recordMillis(duration: Long) {
    recordNanos(TimeUnit.MILLISECONDS.toNanos(duration))
  }

  /**
   * Records an occurrence of the specified duration, in nanoseconds
   */
  def recordNanos(duration: Long) {
    count.getAndIncrement
    totalTimeNanos.getAndAdd(duration)
    minMonitor.record(duration)
    maxMonitor.record(duration)
  }

  /**
   * Returns the total time in nanoseconds
   */
  def getTotalTime: Long = {
    totalTimeMonitor.getValue
  }

  /**
   * Returns the number of measurements that have been made
   */
  def getCount: Long = {
    countMonitor.getValue
  }

  /**
   * Returns the mean recorded time in nanoseconds
   */
  def getMean: Long = {
    meanMonitor.getValue
  }

  /**
   * Returns the maximum recorded time in nanoseconds
   */
  def getMax: Long = {
    maxMonitor.getValue
  }

  /**
   * Returns the minimum recorded time in nanoseconds
   */
  def getMin: Long = {
    minMonitor.getValue
  }

  /**
   * Returns the name of this timer
   */
  def getName: String = {
    baseConfig.getName
  }

  /**
   * Adds the passed-in tag to the config for this timer, and its sub-monitors
   */
  override def addTag(tag: Tag) {
    baseConfig = baseConfig.withAdditionalTag(tag)
    subMonitors.foreach(_.addTag(tag))
  }

  override def getMonitors: util.List[Monitor[_]] = {
    subMonitors
  }

  override def getConfig: MonitorConfig = {
    baseConfig
  }

  /**
   * Returns the result of calling [[getTotalTime]]
   */
  override def getValue: Object = {
    java.lang.Long.valueOf(getTotalTime)
  }

  private def withTag(tag: Tag): MonitorConfig = {
    baseConfig.withAdditionalTag(tag)
  }

  private class LongValueStatMonitor(var config: MonitorConfig, function: () => Long) extends Monitor[Long] with Taggable {
    override def getValue: Long = {
      function.apply()
    }
    override def getConfig: MonitorConfig = {
      config
    }
    override def addTag(tag: Tag) {
      config = config.withAdditionalTag(tag)
    }
  }

  private class ConditionalGauge(var config: MonitorConfig, condition: (Long, Long) => Boolean) extends Gauge[java.lang.Long] with Taggable {
    // This is always set inside a lock but read outside it, so it needs to be volatile
    @volatile var set: Boolean = false
    val value: AtomicLong = new AtomicLong()
    def record(newValue: Long) {
      setInitialValue(newValue)
      while (true) {
        val oldValue = value.get()
        if (condition.apply(oldValue, newValue)) {
          if (value.compareAndSet(oldValue, newValue)) {
            return
          }
        } else {
          return
        }
      }
    }
    override def getValue: java.lang.Long = {
      value.get()
    }
    override def getConfig: MonitorConfig = {
      config
    }
    override def addTag(tag: Tag) {
      config = config.withAdditionalTag(tag)
    }
    private def setInitialValue(newValue: Long) {
      if (!set) {
        this.synchronized {
          // Double-check inside the lock here
          if (!set) {
            set = true
            value.set(newValue)
          }
        }
      }
    }
  }
}

object ServoTimer {

  private final val StatisticTagKey = "statistic"

  private final val TotalTimeTagValue = "totalTime"
  private final val CountTagValue = "count"
  private final val MeanTagValue = "mean"
  private final val MinTagValue = "min"
  private final val MaxTagValue = "max"

  final val NameTagKey = "name"
  final val TimeUnitTagKey = "unit"

  final val TotalTimeTag = newTag(StatisticTagKey, TotalTimeTagValue)
  final val CountTag = newTag(StatisticTagKey, CountTagValue)
  final val MeanTag = newTag(StatisticTagKey, MeanTagValue)
  final val MinTag = newTag(StatisticTagKey, MinTagValue)
  final val MaxTag = newTag(StatisticTagKey, MaxTagValue)

}

trait Taggable {
  def addTag(tag: Tag)
}
