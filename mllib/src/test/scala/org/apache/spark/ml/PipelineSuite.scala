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

package org.apache.spark.ml

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

class PipelineSuite extends SparkFunSuite {

  abstract class MyModel extends Model[MyModel]

  test("pipeline") {
    val estimator0 = mock[Estimator[MyModel]]
    val model0 = mock[MyModel]
    val transformer1 = mock[Transformer]
    val estimator2 = mock[Estimator[MyModel]]
    val model2 = mock[MyModel]
    val transformer3 = mock[Transformer]
    val dataset0 = mock[DataFrame]
    val dataset1 = mock[DataFrame]
    val dataset2 = mock[DataFrame]
    val dataset3 = mock[DataFrame]
    val dataset4 = mock[DataFrame]

    when(estimator0.copy(any[ParamMap])).thenReturn(estimator0)
    when(model0.copy(any[ParamMap])).thenReturn(model0)
    when(transformer1.copy(any[ParamMap])).thenReturn(transformer1)
    when(estimator2.copy(any[ParamMap])).thenReturn(estimator2)
    when(model2.copy(any[ParamMap])).thenReturn(model2)
    when(transformer3.copy(any[ParamMap])).thenReturn(transformer3)

    when(estimator0.fit(meq(dataset0))).thenReturn(model0)
    when(model0.transform(meq(dataset0))).thenReturn(dataset1)
    when(model0.parent).thenReturn(estimator0)
    when(transformer1.transform(meq(dataset1))).thenReturn(dataset2)
    when(estimator2.fit(meq(dataset2))).thenReturn(model2)
    when(model2.transform(meq(dataset2))).thenReturn(dataset3)
    when(model2.parent).thenReturn(estimator2)
    when(transformer3.transform(meq(dataset3))).thenReturn(dataset4)

    val pipeline = new Pipeline()
      .setStages(Array(estimator0, transformer1, estimator2, transformer3))
    val pipelineModel = pipeline.fit(dataset0)

    assert(pipelineModel.stages.length === 4)
    assert(pipelineModel.stages(0).eq(model0))
    assert(pipelineModel.stages(1).eq(transformer1))
    assert(pipelineModel.stages(2).eq(model2))
    assert(pipelineModel.stages(3).eq(transformer3))

    val output = pipelineModel.transform(dataset0)
    assert(output.eq(dataset4))
  }

  test("pipeline with duplicate stages") {
    val estimator = mock[Estimator[MyModel]]
    val pipeline = new Pipeline()
      .setStages(Array(estimator, estimator))
    val dataset = mock[DataFrame]
    intercept[IllegalArgumentException] {
      pipeline.fit(dataset)
    }
  }

  test("PipelineModel.copy") {
    val hashingTF = new HashingTF()
      .setNumFeatures(100)
    val model = new PipelineModel("pipeline", Array[Transformer](hashingTF))
    val copied = model.copy(ParamMap(hashingTF.numFeatures -> 10))
    require(copied.stages(0).asInstanceOf[HashingTF].getNumFeatures === 10,
      "copy should handle extra stage params")
  }
}
