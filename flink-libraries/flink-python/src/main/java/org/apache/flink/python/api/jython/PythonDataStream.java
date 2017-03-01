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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.python.core.PyObject;

import java.io.IOException;
import java.util.ArrayList;

public class PythonDataStream<D extends DataStream> {
	protected final D stream;

	public PythonDataStream(D stream) {
		this.stream = stream;
	}

	public PythonDataStream union(PythonDataStream... streams) {
		ArrayList<DataStream> dsList = new ArrayList<>();
		for (PythonDataStream ps : streams) {
			dsList.add(ps.stream);
		}
		DataStream<PyObject>[] dsArray = new DataStream[dsList.size()];
		return new PythonDataStream(stream.union(dsList.toArray(dsArray)));
	}

	public PythonSplitStream split(OutputSelector<PyObject> selector) throws IOException {
		return new PythonSplitStream(this.stream.split(new PythonOutputSelector(selector)));
	}

	public PythonSingleOutputStreamOperator filter(FilterFunction<PyObject> filter) throws IOException {
		return new PythonSingleOutputStreamOperator(stream.filter(new PythonFilterFunction(filter)));
	}

	public PythonDataStream map(MapFunction fun) throws IOException {
		return new PythonDataStream(stream.map(new PythonMapFunction(fun)));
	}

	public PythonDataStream flat_map(FlatMapFunction fun) throws IOException {
		return new PythonDataStream(stream.flatMap(new PythonFlatMapFunction(fun)));
	}

	public PythonKeyedStream key_by(KeySelector selector) throws IOException {
		return new PythonKeyedStream(stream.keyBy(new PythonKeySelector(selector)));
	}

	public void print() {
		stream.print();
	}

	public void write_as_text(String path) { stream.writeAsText(path); }

	public void write_as_text(String path, WriteMode mode) { stream.writeAsText(path, mode); }

	public void write_to_socket(String host, Integer port, SerializationSchema schema) throws IOException {
		stream.writeToSocket(host, port, new PythonSerializationSchema(schema));
	}

	public void add_sink(SinkFunction fun) throws IOException {
		stream.addSink(new PythonSinkFunction(fun));
	}

	public PythonIterativeStream iterate() { return new PythonIterativeStream(this.stream.iterate()); }

	public PythonIterativeStream iterate(Long max_wait_time_ms) {
		return new PythonIterativeStream(this.stream.iterate(max_wait_time_ms));
	}
}
