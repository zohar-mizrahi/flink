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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyUnicode;
import org.python.core.PyTuple;
import org.python.core.PyObjectDerived;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import java.util.Iterator;

public class PythonStreamExecutionEnvironment {
	private final StreamExecutionEnvironment env;

	public static PythonStreamExecutionEnvironment get_execution_environment() {
		return new PythonStreamExecutionEnvironment();
	}

	public static PythonStreamExecutionEnvironment create_local_execution_environment(Configuration config) {
		return new PythonStreamExecutionEnvironment(config);
	}

	public static PythonStreamExecutionEnvironment create_local_execution_environment(int parallelism, Configuration config) {
		return new PythonStreamExecutionEnvironment(parallelism, config);
	}

	public static PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, String... jarFiles) {
		return new PythonStreamExecutionEnvironment(host, port, jarFiles);
	}

	public static PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, Configuration config, String... jarFiles) {
		return new PythonStreamExecutionEnvironment(host, port, config, jarFiles);
	}

	public static PythonStreamExecutionEnvironment create_remote_execution_environment(
		String host, int port, int parallelism, String... jarFiles) {
		return new PythonStreamExecutionEnvironment(host, port, parallelism, jarFiles);
	}

	private PythonStreamExecutionEnvironment() {
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(Configuration config) {
		this.env = StreamExecutionEnvironment.createLocalEnvironment(config);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(int parallelism, Configuration config) {
		this.env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, config);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(String host, int port, String... jarFiles) {
		this.env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, jarFiles);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(String host, int port, Configuration config, String... jarFiles) {
		this.env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, config, jarFiles);
		this.registerJythonSerializers();
	}

	private PythonStreamExecutionEnvironment(String host, int port, int parallelism, String... jarFiles) {
		this.env = StreamExecutionEnvironment.createRemoteEnvironment(host, port, parallelism, jarFiles);
		this.registerJythonSerializers();
	}

	private void registerJythonSerializers() {
		this.env.registerTypeWithKryoSerializer(PyString.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyInteger.class, PyObjectSerializer.class);
		this.env.registerTypeWithKryoSerializer(PyLong.class, PyObjectSerializer.class);
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

	public PythonDataStream from_collection(Collection<Object> collection) {
		return new PythonDataStream(env.fromCollection(collection).map(new UtilityFunctions.SerializerMap<>()));
	}

	public PythonDataStream from_collection(Iterator<Object> iter) throws Exception  {
		return new PythonDataStream(env.fromCollection(new PythonIteratorFunction(iter), Object.class)
			.map(new UtilityFunctions.SerializerMap<>()));
	}

	public PythonDataStream generate_sequence(long from, long to) {
		return new PythonDataStream(env.generateSequence(from, to).map(new UtilityFunctions.SerializerMap<Long>()));
	}

	public PythonDataStream read_text_file(String path) throws IOException {
		return new PythonDataStream(env.readTextFile(path).map(new UtilityFunctions.SerializerMap<String>()));
	}

	public PythonDataStream socket_text_stream(String host, int port) {
		return new PythonDataStream(env.socketTextStream(host, port).map(new UtilityFunctions.SerializerMap<String>()));
	}

	public PythonStreamExecutionEnvironment enable_checkpointing(long interval) {
		this.env.enableCheckpointing(interval);
		return this;
	}

	public PythonStreamExecutionEnvironment enable_checkpointing(long interval, CheckpointingMode mode) {
		this.env.enableCheckpointing(interval, mode);
		return this;
	}

	public PythonStreamExecutionEnvironment set_parallelism(int parallelism) {
		this.env.setParallelism(parallelism);
		return this;
	}

	public JobExecutionResult execute() throws Exception {
		return this.env.execute();
	}

	public JobExecutionResult execute(String job_name) throws Exception {
		return this.env.execute(job_name);
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
