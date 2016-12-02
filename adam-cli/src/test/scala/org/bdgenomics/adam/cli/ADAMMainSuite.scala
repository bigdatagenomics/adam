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

import java.io.ByteArrayOutputStream
import com.google.inject.{ AbstractModule, Guice }
import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.InjectorExtensions._
import org.bdgenomics.adam.cli.ADAMMain.defaultCommandGroups
import org.scalatest.FunSuite

class ADAMMainSuite extends FunSuite {

  test("default command groups is non empty") {
    assert(defaultCommandGroups.nonEmpty)
  }

  test("module provides default command groups") {
    val injector = Guice.createInjector(new ADAMModule())
    val commandGroups = injector.instance[List[CommandGroup]]
    assert(commandGroups == defaultCommandGroups)
  }

  test("inject default command groups when called via main") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      ADAMMain.main(Array())
    }
    val out = stream.toString()
    // the help text has been displayed
    assert(out.contains("Usage"))
    // ...and flatten (from default groups) is one of the commands listed
    assert(out.contains("flatten"))
  }

  test("command groups is empty when called via apply") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      new ADAMMain(List.empty)(Array())
    }
    val out = stream.toString()
    assert(out.contains("Usage"))
    assert(!out.contains("flatten"))
  }

  test("single command group") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      new ADAMMain(List(CommandGroup("SINGLE COMMAND GROUP", List(Flatten)))).apply(Array())
    }
    val out = stream.toString()
    assert(out.contains("Usage"))
    assert(out.contains("SINGLE"))
    assert(out.contains("flatten"))
  }

  test("add new command group to default command groups") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      val commandGroups = defaultCommandGroups.union(List(CommandGroup("NEW COMMAND GROUP", List(Flatten))))
      new ADAMMain(commandGroups)(Array())
    }
    val out = stream.toString()
    assert(out.contains("Usage"))
    assert(out.contains("NEW"))
  }

  test("module restores default command groups when called via apply") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      val injector = Guice.createInjector(new ADAMModule())
      val commandGroups = injector.instance[List[CommandGroup]]
      new ADAMMain(commandGroups).apply(Array())
    }
    val out = stream.toString()
    assert(out.contains("Usage"))
    assert(out.contains("flatten"))
  }

  test("custom module with single command group") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      val module = new AbstractModule with ScalaModule {
        def configure() = {
          bind[List[CommandGroup]].toInstance(List(CommandGroup("SINGLE COMMAND GROUP", List(Flatten))))
        }
      }
      val injector = Guice.createInjector(module)
      val commandGroups = injector.instance[List[CommandGroup]]
      new ADAMMain(commandGroups).apply(Array())
    }
    val out = stream.toString()
    assert(out.contains("Usage"))
    assert(out.contains("SINGLE"))
    assert(out.contains("flatten"))
  }

  test("custom module with new command group added to default command groups") {
    val stream = new ByteArrayOutputStream()
    Console.withOut(stream) {
      val module = new AbstractModule with ScalaModule {
        def configure() = {
          bind[List[CommandGroup]].toInstance(defaultCommandGroups.union(List(CommandGroup("NEW COMMAND GROUP", List(Flatten)))))
        }
      }
      val injector = Guice.createInjector(module)
      val commandGroups = injector.instance[List[CommandGroup]]
      new ADAMMain(commandGroups).apply(Array())
    }
    val out = stream.toString()
    assert(out.contains("Usage"))
    assert(out.contains("NEW"))
  }
}
