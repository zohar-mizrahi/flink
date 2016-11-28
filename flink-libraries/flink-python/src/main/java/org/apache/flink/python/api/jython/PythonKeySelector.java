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

import java.io.IOException;

public class PythonKeySelector implements KeySelector<byte[], PyKey> {
	private static final long serialVersionUID = 7403775239671366607L;
	private final byte[] serFun;
	private transient KeySelector<Object, Object> fun;

	public PythonKeySelector(KeySelector<Object, Object> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	@Override
	public PyKey getKey(byte[] value) throws Exception {
		if (fun == null) {
			fun = (KeySelector<Object, Object>) SerializationUtils.deserializeObject(serFun);
		}
		Object key = fun.getKey(SerializationUtils.deserializeObject(value));
		return new PyKey(SerializationUtils.serializeObject(key));
	}
}
