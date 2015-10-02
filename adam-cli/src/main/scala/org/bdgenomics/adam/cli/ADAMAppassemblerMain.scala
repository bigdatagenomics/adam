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
package org.bdgenomics.adam.cli

object ADAMAppassemblerMain {

  def main(args: Array[String]) {
    println("\n")
    println("""     e            888~-_              e                 e    e
               |    d8b           888   \            d8b               d8b  d8b
               |   /Y88b          888    |          /Y88b             d888bdY88b
               |  /  Y88b         888    |         /  Y88b           / Y88Y Y888b
               | /____Y88b        888   /         /____Y88b         /   YY   Y888b
               |/      Y88b       888_-~         /      Y88b       /          Y888b""".stripMargin('|'))
    println("\nUsage: adam-shell [<spark-args>]")
    println(" or")
    println("Usage: adam-submit [<spark-args> --] <adam-args>")
    println("\n")
  }
}
