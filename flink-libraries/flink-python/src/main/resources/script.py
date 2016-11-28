from org.apache.flink.api.common.functions import MapFunction, FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.python.api.jython import PythonStreamExecutionEnvironment
from org.apache.flink.streaming.api.windowing.time.Time import seconds

class Map(MapFunction):
	def map(self, input):
		return input + 1

class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        for word in value.lower().split():
            collector.collect((1, word))

class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, word1 = input1
        count2, word2 = input2
        return (count1 + count2, word1)

class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


env = PythonStreamExecutionEnvironment.get_execution_environment()

env.create_predefined_java_source() \
    .flat_map(Tokenizer()) \
    .key_by(Selector()) \
    .time_window(seconds(1)) \
    .reduce(Sum()) \
    .print()

env.execute()
