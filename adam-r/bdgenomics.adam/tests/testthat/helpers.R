#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

resourceFile <- function(fileName, submodule="adam-core") {
    file.path(dirname(dirname(dirname(dirname(getwd())))),
              paste(submodule, "src/test/resources", sep = "/",
                    fileName))
}

expect_files_match <- function(newFile, originalFile) {
    expect_equal(readLines(newFile), readLines(originalFile))
}
