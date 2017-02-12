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

import java.io.Serializable;
import java.util.Iterator;
import java.io.IOException;

public class PythonIteratorFunction implements Iterator<Object>, Serializable {
	private static final long serialVersionUID = 6741748297048588334L;

	private final byte[] serFun;
	private transient Iterator<Object> fun;

	public PythonIteratorFunction(Iterator<Object> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	public boolean hasNext() {
		if (this.fun == null) {
			extractUserFunction();
		}

		return this.fun.hasNext();
	}

	public Object next(){
		if (this.fun == null) {
			extractUserFunction();
		}

		return UtilityFunctions.adapt(this.fun.next());
	}

	public void remove() {
	}

	private void extractUserFunction() {
		try {
			this.fun = (Iterator<Object>) SerializationUtils.deserializeObject(this.serFun);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
