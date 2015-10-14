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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types._
import org.scalatest.{Matchers, FunSpec}

class RowTest extends FunSpec with Matchers {

  val schema = StructType(
    StructField("col1", StringType) ::
    StructField("col2", StringType) ::
    StructField("col3", IntegerType) :: Nil)
  val values = Array("value1", "value2", 1)

  val sampleRow: Row = new GenericRowWithSchema(values, schema)
  val noSchemaRow: Row = new GenericRow(values)

  describe("Row (without schema)") {
    it("throws an exception when accessing by fieldName") {
      intercept[UnsupportedOperationException] {
        noSchemaRow.fieldIndex("col1")
      }
      intercept[UnsupportedOperationException] {
        noSchemaRow.getAs("col1")
      }
    }
  }

  describe("Row (with schema)") {
    it("fieldIndex(name) returns field index") {
      sampleRow.fieldIndex("col1") shouldBe 0
      sampleRow.fieldIndex("col3") shouldBe 2
    }

    it("getAs[T] retrieves a value by fieldname") {
      sampleRow.getAs[String]("col1") shouldBe "value1"
      sampleRow.getAs[Int]("col3") shouldBe 1
    }

    it("Accessing non existent field throws an exception") {
      intercept[IllegalArgumentException] {
        sampleRow.getAs[String]("non_existent")
      }
    }

    it("getValuesMap() retrieves values of multiple fields as a Map(field -> value)") {
      val expected = Map(
        "col1" -> "value1",
        "col2" -> "value2"
      )
      sampleRow.getValuesMap(List("col1", "col2")) shouldBe expected
    }
  }
}
