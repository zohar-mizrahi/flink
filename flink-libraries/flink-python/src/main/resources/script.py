from org.apache.flink.api.common.functions import MapFunction
from org.apache.flink.python.api.jython import PythonStreamExecutionEnvironment

class Map(MapFunction):
	def map(self, input):
		return input + 1

env = PythonStreamExecutionEnvironment.get_execution_environment()

env.from_elements(0, 1, 2).map(Map()).print()

env.execute()
