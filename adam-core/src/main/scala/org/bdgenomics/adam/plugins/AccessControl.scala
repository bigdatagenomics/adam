/**
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.plugins

/**
 * This allows filtering based on an input type.
 */
trait AccessControl[Input] {
  /**
   * The predicate associated with the current AccessControl.
   * @return If there is no filter for AccessControl, None
   *         If a filter is provided, a true response means access is granted; a false means access is denied.
   */
  def predicate: Option[Input => Boolean]
}
