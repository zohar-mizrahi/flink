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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;

public class PythonDataStream<D extends DataStream> {
	protected final D stream;

	public PythonDataStream(D stream) {
		this.stream = stream;
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

	public static class PythonKeyedStream extends PythonDataStream<KeyedStream> {

		public PythonKeyedStream(KeyedStream stream) {
			super(stream);
		}

		public PythonWindow count_window(long size, long slide) {
			return new PythonWindow(stream.countWindow(size, slide));
		}

		public PythonWindow time_window(Time size) {
			return new PythonWindow(stream.timeWindow(size));
		}

		public PythonWindow time_window(Time size, Time slide) {
			return new PythonWindow(stream.timeWindow(size, slide));
		}
	}

	public static class PythonSingleOutputStreamOperator extends PythonDataStream<SingleOutputStreamOperator> {
		public PythonSingleOutputStreamOperator(SingleOutputStreamOperator stream) {
			super(stream);
		}
	}

	public static class PythonWindow {
		private final WindowedStream stream;

		public PythonWindow(WindowedStream stream) {
			this.stream = stream;
		}

		public PythonSingleOutputStreamOperator reduce(ReduceFunction fun) throws IOException {
			return new PythonSingleOutputStreamOperator(stream.reduce(new PythonReduceFunction(fun)));
		}
	}
}
