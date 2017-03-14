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
from utils.python_test_base import TestBase
from org.apache.flink.streaming.api.functions.source import SourceFunction



class Generator1(SourceFunction):
    def __init__(self, num_iters=7000):
        self._running = True
        self._num_iters = num_iters

    def run(self, ctx):
        counter = 0
        while self._running and counter < self._num_iters:
            self.do(ctx)
            counter += 1

    def cancel(self):
        self._running = False

    def do(self, ctx):
        ctx.collect(222)


class Main(TestBase):
    def __init__(self):
        super(Main, self).__init__()

    def run(self):
        elements = [111 if iii % 2 == 0 else 2222 for iii in range(100)]

        env = self._get_execution_environment()
        env.create_python_source(Generator1(100)) \
            .print()
        # env.from_elements(*elements) \
        #    .print()

        result = env.execute("MyJob", True)
        print("Job completed, job_id={}".format(result.jobID))


if __name__ == '__main__':
    Main().run()
