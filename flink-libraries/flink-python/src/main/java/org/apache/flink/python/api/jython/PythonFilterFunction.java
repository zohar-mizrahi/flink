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
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.python.core.PyObject;

import java.io.IOException;

public class PythonFilterFunction extends RichFilterFunction<PyObject> {
	private static final long serialVersionUID = 775688642701399472L;

	private final byte[] serFun;
	private transient FilterFunction<PyObject> fun;

	public PythonFilterFunction(FilterFunction<PyObject> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	public void open(Configuration parameters) throws IOException, ClassNotFoundException{
		this.fun = (FilterFunction<PyObject>) SerializationUtils.deserializeObject(this.serFun);
	}


	@Override
	public boolean filter(PyObject value) throws Exception {
		return this.fun.filter(value);
	}
}