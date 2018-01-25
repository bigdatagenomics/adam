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
package org.bdgenomics.adam.rdd

import org.bdgenomics.utils.cli.SaveArgs

/**
 * Argument configuration for saving any output format.
 */
trait ADAMSaveAnyArgs extends SaveArgs {

  /**
   * If true and saving as FASTQ, we will sort by read name.
   */
  var sortFastqOutput: Boolean

  /**
   * If true and saving as a legacy format, we will write shards so that they
   * can be merged into a single file.
   *
   * @see deferMerging
   */
  var asSingleFile: Boolean

  /**
   * If true and asSingleFile is true, we will not merge the shards once we
   * write them, and will leave them for the user to merge later. If false and
   * asSingleFile is true, then we will merge the shards on write. If
   * asSingleFile is false, this is ignored.
   *
   * @see asSingleFile
   */
  var deferMerging: Boolean

  /**
   * If asSingleFile is true and deferMerging is false, disables the use of the
   * fast file concatenation engine.
   */
  var disableFastConcat: Boolean
}
