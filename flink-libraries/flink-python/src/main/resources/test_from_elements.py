################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import sys
from pygeneratorbase import PyGeneratorBase
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.python.api.jython import PythonStreamExecutionEnvironment
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds
from org.apache.flink.api.java.utils import ParameterTool


class Generator(PyGeneratorBase):
    def __init__(self):
        super(Generator, self).__init__()

    def do(self, ctx):
        ctx.collect(222)

class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        collector.collect((1, value))

class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)

class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


def main():
    params = ParameterTool.fromArgs(sys.argv[1:])
    env = PythonStreamExecutionEnvironment.create_local_execution_environment(params.getConfiguration())

    elements = []
    for iii in range(1000):
        elements.append(111 if iii % 2 == 0 else 2222)

    env.from_elements(*elements) \
        .flat_map(Tokenizer()) \
        .key_by(Selector()) \
        .time_window(milliseconds(10)) \
        .reduce(Sum()) \
        .print()

    result = env.execute("MyJob")
    print("Job completed, job_id={}".format(result.jobID))


if __name__ == '__main__':
    main()