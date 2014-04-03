/*
 * Copyright 2014 Genome Bridge LLC
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
package org.bdgenomics.adam.util

import java.util.regex._

object RegExp {
  def apply(pattern : String) : RegExp = new RegExp(pattern)
}

/**
 * Wraps the java Pattern class, to allow for easier regular expression matching
 * (including making the matches/finds methods return Option[Matcher], so that we can
 * flatMap a set of strings with these methods).
 *
 * @param patternString The Pattern-compatiable regular expression to be compiled and used for matching.
 */
class RegExp(val patternString : String) {
  val pattern = Pattern.compile(patternString)

  def matches(tgt : String) : Option[Matcher] = {
    val m = pattern.matcher(tgt)
    if(m.matches()) {
      Some(m)
    } else {
      None
    }
  }

  def find(tgt : String, idx : Int = 0) : Option[Matcher] = {
    val m = pattern.matcher(tgt)
    if(m.find(idx)) {
      Some(m)
    } else {
      None
    }
  }
}
