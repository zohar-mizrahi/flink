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

import org.apache.flink.api.common.functions.MapFunction;

public class UtilityFunctions {
	private UtilityFunctions() {
	}

	public static class SerializerMap<IN> implements MapFunction<IN, byte[]> {
		private static final long serialVersionUID = 1582769662549499373L;

		@Override
		public byte[] map(IN value) throws Exception {
			return SerializationUtils.serializeObject(value);
		}
	}

	public static class DeserializerStringifyMap implements MapFunction<byte[], String> {
		private static final long serialVersionUID = -737690187885560482L;

		@Override
		public String map(byte[] value) throws Exception {
			return SerializationUtils.deserializeObject(value).toString();
		}
	}
}
