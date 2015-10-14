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

package org.apache.spark.sql.types

import org.json4s.JsonDSL._

import org.apache.spark.annotation.DeveloperApi


object ArrayType {
  /** Construct a [[ArrayType]] object with the given element type. The `containsNull` is true. */
  def apply(elementType: DataType): ArrayType = ArrayType(elementType, containsNull = true)
}


/**
 * :: DeveloperApi ::
 * The data type for collections of multiple values.
 * Internally these are represented as columns that contain a ``scala.collection.Seq``.
 *
 * Please use [[DataTypes.createArrayType()]] to create a specific instance.
 *
 * An [[ArrayType]] object comprises two fields, `elementType: [[DataType]]` and
 * `containsNull: Boolean`. The field of `elementType` is used to specify the type of
 * array elements. The field of `containsNull` is used to specify if the array has `null` values.
 *
 * @param elementType The data type of values.
 * @param containsNull Indicates if values have `null` values
 *
 * @group dataType
 */
@DeveloperApi
case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType {

  /** No-arg constructor for kryo. */
  protected def this() = this(null, false)

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    builder.append(
      s"$prefix-- element: ${elementType.typeName} (containsNull = $containsNull)\n")
    DataType.buildFormattedString(elementType, s"$prefix    |", builder)
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("elementType" -> elementType.jsonValue) ~
      ("containsNull" -> containsNull)

  /**
   * The default size of a value of the ArrayType is 100 * the default size of the element type.
   * (We assume that there are 100 elements).
   */
  override def defaultSize: Int = 100 * elementType.defaultSize

  override def simpleString: String = s"array<${elementType.simpleString}>"

  private[spark] override def asNullable: ArrayType =
    ArrayType(elementType.asNullable, containsNull = true)
}
