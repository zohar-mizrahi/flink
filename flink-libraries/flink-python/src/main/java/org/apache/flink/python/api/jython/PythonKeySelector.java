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

import org.apache.flink.api.java.functions.KeySelector;
import org.python.core.PyObject;

import java.io.IOException;

public class PythonKeySelector implements KeySelector<PyObject, PyKey> {
	private static final long serialVersionUID = 7403775239671366607L;
	private final byte[] serFun;
	private transient KeySelector<PyObject, Object> fun;

	public PythonKeySelector(KeySelector<PyObject, PyKey> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	public PyKey getKey(PyObject value) throws Exception {
		if (fun == null) {
			fun = (KeySelector<PyObject, Object>) SerializationUtils.deserializeObject(serFun);
		}
		Object key = fun.getKey(value);
		return new PyKey(key);
	}
}
