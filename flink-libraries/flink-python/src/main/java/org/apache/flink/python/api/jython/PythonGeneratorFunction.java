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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.IOException;

public class PythonGeneratorFunction implements SourceFunction<Object> {
	private static final long serialVersionUID = 3854587935845323082L;

	private final byte[] serFun;
	private transient SourceFunction<Object> fun;

	public PythonGeneratorFunction(SourceFunction<Object> fun) throws IOException {
		this.serFun = SerializationUtils.serializeObject(fun);
	}

	public void run(SourceContext<Object> ctx) throws Exception {
		if (this.fun == null) {
			this.fun = (SourceFunction<Object> ) SerializationUtils.deserializeObject(this.serFun);
		}

		this.fun.run(ctx);
	}

	public void cancel() {
		if (this.fun != null) {
			this.fun.cancel();
		}
	}
}
