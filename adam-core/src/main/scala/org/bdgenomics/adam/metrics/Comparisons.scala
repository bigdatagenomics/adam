/*
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.metrics

import org.bdgenomics.adam.models.ReadBucket
import org.bdgenomics.adam.projections.FieldValue
import java.util.regex.Pattern
import java.io.Writer
import org.bdgenomics.adam.metrics.filters.ComparisonsFilter

trait BucketComparisons[+T] {
  /**
   * Name of the comparison. Should be identifiable, and able to be written on the command line.
   */
  def name: String

  /**
   * Description of the comparison, to be used by the "list comparisons" CLI option.
   */
  def description: String

  /**
   * All of the schemas which this comparison uses.
   */
  def schemas: Seq[FieldValue]

  /**
   * The records have been matched by their names, but the rest may be mismatched.
   */
  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Seq[T]

  /**
   * parses and creates a filter for this BucketComparisons value, conforming to the definition in
   * the filterDef argument.
   *
   * @param filterDef a filter definition string, e.g. ">3"
   * @return A ComparisonsFilter value that performs the indicated comparison on the
   *         output of 'this' (i.e. T values)
   */
  def createFilter(filterDef: String): ComparisonsFilter[Any]

}

object BucketComparisons {

  val pointRegex = Pattern.compile("\\(\\s*(-?\\d+)\\s*,\\s*(-?\\d+)\\s*\\)")

  def parsePoint(pointString: String): (Int, Int) = {

    val pointMatcher = pointRegex.matcher(pointString)
    if (!pointMatcher.matches()) {
      throw new IllegalArgumentException("\"%s\" doesn't match point regex".format(pointString))
    }

    val p1 = pointMatcher.group(1).toInt
    val p2 = pointMatcher.group(2).toInt

    (p1, p2)
  }

  case class ParsedFilter(filter: String, value: String) {
    def valueAsInt: Int = value.toInt
    def valueAsDouble: Double = value.toDouble
    def valueAsPoint: (Int, Int) = parsePoint(value)
    def valueAsBoolean: Boolean = value.toBoolean
    def valueAsLong: Long = value.toLong

    def assertFilterValues(filterName: String, legalValues: String*) {
      if (!legalValues.contains(filter)) {
        throw new IllegalArgumentException("\"%s\" isn't a legal filter value for %s".format(value, filterName))
      }
    }
  }

  val filterStringRegex = Pattern.compile("(!=|=|<|>)(.*)")

  def parseFilterString(filterString: String): ParsedFilter = {
    val matcher = filterStringRegex.matcher(filterString)
    if (!matcher.matches()) {
      throw new IllegalArgumentException("\"%s\" doesn't match filter string pattern".format(filterString))
    }

    ParsedFilter(matcher.group(1), matcher.group(2))
  }

  class EqualityFilter[T](generator: BucketComparisons[T], target: T) extends ComparisonsFilter[T](generator) {
    def passesFilter(value: Any): Boolean = value == target
  }

  class InequalityFilter[T](generator: BucketComparisons[T], target: T) extends ComparisonsFilter[T](generator) {
    def passesFilter(value: Any): Boolean = value != target
  }

  class WrapperFilter[T](generator: BucketComparisons[T], f: (Any) => Boolean) extends ComparisonsFilter[T](generator) {
    def passesFilter(value: Any): Boolean = f(value)
  }
}

class Collection[T](val values: Seq[T]) extends Serializable {
}

object Collection {
  def apply[T](values: Traversable[T]): Collection[T] = new Collection[T](values.toSeq)
}

class CombinedComparisons[T](inner: Seq[BucketComparisons[T]]) extends BucketComparisons[Collection[Seq[T]]] with Serializable {
  /**
   * Name of the comparison. Should be identifiable, and able to be written on the command line.
   */
  def name: String = "(%s)".format(inner.map(_.name).reduce("%s+%s".format(_, _)))

  /**
   * Description of the comparison, to be used by the "list comparisons" CLI option.
   */
  def description: String = "A list of comparisons"

  /**
   * All of the schemas which this comparison uses.
   */
  def schemas: Seq[FieldValue] = inner.flatMap(_.schemas)

  /**
   * The records have been matched by their names, but the rest may be mismatched.
   */
  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Seq[Collection[Seq[T]]] =
    Seq(Collection(inner.map(bc => bc.matchedByName(bucket1, bucket2))))

  /**
   * parses and creates a filter for this BucketComparisons value, conforming to the definition in
   * the filterDef argument.
   *
   * @param filterDef a filter definition string, e.g. ">3"
   * @return A ComparisonsFilter value that performs the indicated comparison on the
   *         output of 'this' (i.e. T values)
   */
  def createFilter(filterDef: String): ComparisonsFilter[Any] = {
    throw new UnsupportedOperationException()
  }
}

abstract class BooleanComparisons extends BucketComparisons[Boolean] {

  /**
   * parses and creates a filter for this BucketComparisons value, conforming to the definition in
   * the filterDef argument.
   *
   * @param filterDef a filter definition string, e.g. ">3"
   * @return A ComparisonsFilter value that performs the indicated comparison on the
   *         output of 'this' (i.e. T values)
   */
  def createFilter(filterDef: String): ComparisonsFilter[Boolean] = {
    import BucketComparisons._

    val parsedFilter = parseFilterString(filterDef)
    parsedFilter.assertFilterValues(name, "=")
    new EqualityFilter(this, parsedFilter.valueAsBoolean)
  }
}

abstract class LongComparisons extends BucketComparisons[Long] {

  /**
   * parses and creates a filter for this BucketComparisons value, conforming to the definition in
   * the filterDef argument.
   *
   * @param filterDef a filter definition string, e.g. ">3"
   * @return A ComparisonsFilter value that performs the indicated comparison on the
   *         output of 'this' (i.e. T values)
   */
  def createFilter(filterDef: String): ComparisonsFilter[Long] = {
    import BucketComparisons._

    val parsedFilter = parseFilterString(filterDef)
    parsedFilter.assertFilterValues(name, "=", ">", "<")

    parsedFilter.filter match {
      case "=" => new EqualityFilter(this, parsedFilter.valueAsLong)
      case ">" => new WrapperFilter(this, (value: Any) => value.asInstanceOf[Long] > parsedFilter.valueAsLong)
      case "<" => new WrapperFilter(this, (value: Any) => value.asInstanceOf[Long] < parsedFilter.valueAsLong)

      case _ => throw new IllegalArgumentException(
        "Unknown filter-type \"%s\" in filterDef \"%s\"".format(parsedFilter.filter, filterDef))
    }
  }
}

abstract class PointComparisons extends BucketComparisons[(Int, Int)] {

  /**
   * parses and creates a filter for this BucketComparisons value, conforming to the definition in
   * the filterDef argument.
   *
   * @param filterDef a filter definition string, e.g. ">3"
   * @return A ComparisonsFilter value that performs the indicated comparison on the
   *         output of 'this' (i.e. T values)
   */
  def createFilter(filterDef: String): ComparisonsFilter[(Int, Int)] = {
    import BucketComparisons._

    val parsedFilter = parseFilterString(filterDef)
    parsedFilter.assertFilterValues(name, "=", "!=")
    if (parsedFilter.filter == "=") new EqualityFilter(this, parsedFilter.valueAsPoint)
    else new InequalityFilter(this, parsedFilter.valueAsPoint)
  }
}

