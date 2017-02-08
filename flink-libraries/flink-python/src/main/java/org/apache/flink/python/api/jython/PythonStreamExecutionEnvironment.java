/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flink.python.api.jython;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.python.core.PyObject;
import org.python.core.PyObjectDerived;
import org.python.core.PyInteger;
import org.python.core.PyTuple;
import org.python.core.PyUnicode;

public class PythonStreamExecutionEnvironment {
	private final StreamExecutionEnvironment env;

	public static PythonStreamExecutionEnvironment get_execution_environment() {
		return new PythonStreamExecutionEnvironment();
	}

	public static PythonStreamExecutionEnvironment create_local_execution_environment(Configuration config) {
		return new PythonStreamExecutionEnvironment(config);
	}

	private PythonStreamExecutionEnvironment() {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(Configuration config) {
		this.env = StreamExecutionEnvironment.createLocalEnvironment(config);
		this.registerJythonSerializers();
	}

	private void registerJythonSerializers() {
		this.env.registerTypeWithKryoSerializer(PyInteger.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyUnicode.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyTuple.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyObjectDerived.class, PyObjectSerializer.class);
	}

	public PythonDataStream create_predefined_java_source(Integer num_iters) {
		return new PythonDataStream(env.addSource(new TempSource(num_iters)).map(new UtilityFunctions.SerializerMap<>()));
	}

	public PythonDataStream create_python_source(SourceFunction<Object> src) throws Exception {
		return new PythonDataStream(env.addSource(new PythonGeneratorFunction(src)).map(new UtilityFunctions.SerializerMap<>()));
	}

	public PythonDataStream from_elements(PyObject... elements) {
		return new PythonDataStream(env.fromElements(elements));
	}

	public PythonDataStream create_predefined_java_source(Integer num_iters) {
		return new PythonDataStream(env.addSource(new TempSource(num_iters)).map(new UtilityFunctions.SerializerMap<>()));
	}

	public PythonDataStream create_python_source(SourceFunction<Object> src) throws Exception {
		return new PythonDataStream(env.addSource(new PythonGenerator(src)).map(new UtilityFunctions.SerializerMap<>()));
	}

	public void execute() throws Exception {
		this.env.execute();
	}

	public static class TempSource implements SourceFunction<Object> {
		private boolean running = true;
		private Integer num_iters;

		public TempSource(Integer num_iters) {
			this.num_iters = num_iters;
		}

		@Override
		public void run(SourceContext<Object> ctx) throws Exception {
			Integer counter = 0;
			boolean run_forever = (num_iters == -1);
			while (running && (run_forever || counter++ < this.num_iters)){
				ctx.collect("World");
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
