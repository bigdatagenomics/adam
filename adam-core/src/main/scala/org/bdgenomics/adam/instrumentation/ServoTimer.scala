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

import com.netflix.servo.monitor.{ CompositeMonitor, Gauge, Monitor, MonitorConfig }
import com.netflix.servo.tag.Tags.newTag
import com.netflix.servo.tag.{ Tag, Tags }
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.AtomicLong
import org.bdgenomics.adam.instrumentation.ServoTimer._
import scala.collection.JavaConversions._
import scala.collection.mutable

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
 * @param name the name of this timer; this is propagated to sub-monitors
 * @param tags the tags for this timer; these are propagated to sub-monitors
 */
class ServoTimer(name: String, @transient tags: Tag*) extends ConfigurableMonitor(name, tags)
    with CompositeMonitor[Object] {

  private val totalTimeNanos = new AtomicLong()
  private val count = new AtomicLong()

  private val totalTimeMonitor = new LongValueStatMonitor(name, withTag(TotalTimeTag), () => totalTimeNanos.longValue())
  private val countMonitor = new LongValueStatMonitor(name, withTag(CountTag), () => count.longValue())

  private val meanMonitor = new LongValueStatMonitor(name, withTag(MeanTag), () => {
    if (count.longValue() > 0) {
      (totalTimeNanos.doubleValue() / count.doubleValue()).toLong
    } else {
      0
    }
  })
  private val minMonitor = new ConditionalGauge(name, withTag(MinTag),
    (existingValue, newValue) => { newValue < existingValue })
  private val maxMonitor = new ConditionalGauge(name, withTag(MaxTag),
    (existingValue, newValue) => { newValue > existingValue })

  private val subMonitors: mutable.Buffer[Monitor[_]] =
    mutable.ArrayBuffer(totalTimeMonitor, countMonitor, meanMonitor, minMonitor, maxMonitor)

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
   * Adjusts the total time. The mean time will be computed using the new total, and the
   * count is left unchanged. However, the min and max values are cleared, as it is no
   * longer possible to determine them after the total is adjusted.
   */
  def adjustTotalTime(duration: Long) {
    totalTimeNanos.getAndAdd(duration)
    // We need to wipe the min and max here, as they will be wrong
    minMonitor.clear()
    maxMonitor.clear()
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
   * Merges the passed-in [[ServoTimer]] into this one. Note that the config is not merged (only the recorded statistics).
   */
  def merge(mergeWith: ServoTimer) {
    totalTimeNanos.getAndAdd(mergeWith.totalTimeNanos.get())
    count.getAndAdd(mergeWith.count.get())
    minMonitor.record(mergeWith.minMonitor.getValue)
    maxMonitor.record(mergeWith.maxMonitor.getValue)
  }

  def addSubMonitor(monitor: Monitor[_]) {
    subMonitors += monitor
  }

  override def getMonitors: util.List[Monitor[_]] = {
    subMonitors
  }

  /**
   * Returns the result of calling [[getTotalTime]]
   */
  override def getValue: Object = {
    java.lang.Long.valueOf(getTotalTime)
  }

  private def withTag(tag: Tag): Seq[Tag] = {
    tags ++ Seq(tag)
  }

  private class LongValueStatMonitor(name: String, @transient tags: Seq[Tag], function: () => Long)
      extends ConfigurableMonitor(name, tags) with Monitor[Long] {
    override def getValue: Long = {
      function.apply()
    }
  }

  private class ConditionalGauge(name: String, @transient tags: Seq[Tag], condition: (Long, Long) => Boolean)
      extends ConfigurableMonitor(name, tags) with Gauge[java.lang.Long] {
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
    def clear() {
      this.synchronized {
        set = false
        value.set(0)
      }
    }
    override def getValue: java.lang.Long = {
      if (set) value.get() else null
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

abstract class ConfigurableMonitor(val name: String, @transient tags: Seq[Tag])
    extends Taggable with Serializable {

  private val serializableTags: mutable.Buffer[SerializableTag] = createSerializableTags(tags)

  // This is created at construction time, and (when the object is deserialized) by the readResolve method
  @transient private var baseConfig: MonitorConfig = createBaseConfig(name, serializableTags)

  def getName: String = {
    name
  }

  override def addTag(tag: Tag) {
    baseConfig = getConfig.withAdditionalTag(tag)
    serializableTags += new SerializableTag(tag.getKey, tag.getValue)
  }

  def getConfig: MonitorConfig = {
    if (baseConfig == null) {
      baseConfig = createBaseConfig(name, serializableTags)
    }
    baseConfig
  }

  private def createSerializableTags(tags: Seq[Tag]): mutable.Buffer[SerializableTag] = {
    tags.map(tag => new SerializableTag(tag.getKey, tag.getValue)).toBuffer
  }

  private def createBaseConfig(name: String, tags: Seq[SerializableTag]): MonitorConfig = {
    val builder = MonitorConfig.builder(name)
    tags.foreach(tag => { builder.withTag(Tags.newTag(tag.key, tag.value)) })
    builder.withTag(TimeUnitTagKey, NANOSECONDS.name()).withTag(NameTagKey, name)
    builder.build()
  }

}

private class SerializableTag(val key: String, val value: String) extends Serializable

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
