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

import org.python.util.PythonInterpreter;

import java.io.File;

public class PythonStreamBinder {
	private PythonStreamBinder() {
	}

	public static void main(String[] args) throws Exception {
		PythonInterpreter.initialize(System.getProperties(), System.getProperties(), new String[0]);
		try (PythonInterpreter interpreter = new PythonInterpreter()) {
			interpreter.setErr(System.err);
			interpreter.setOut(System.out);

			File script = new File(PythonStreamBinder.class.getClassLoader().getResource("script.py").getFile());
			interpreter.execfile(script.getAbsolutePath());
			// interpreter.execfile(args[0]);
		}
	}
}
