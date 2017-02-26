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
import java.io.FileNotFoundException;
import java.util.Properties;

public class PythonStreamBinder {

	private PythonStreamBinder() {
	}

	/**
	 * Entry point for the execution of a python streaming task.
	 *
	 * @param args <pathToScript> [parameter1]..[parameterX]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		File script;
		if (args.length < 1) {
			System.out.println("Usage: prog <pathToScript> [parameter1]..[parameterX]");
			return;
		}
		else {
			script = new File(args[0]);
			if ((!script.exists()) || (!script.isFile()))
			{
				throw new FileNotFoundException("Could not find file: " + args[0]);
			}

		}
		Properties props = System.getProperties();
		props.put("python.path", script.getParent());
		PythonInterpreter.initialize(props, props, args);
		try (PythonInterpreter interpreter = new PythonInterpreter()) {
			interpreter.setErr(System.err);
			interpreter.setOut(System.out);
			interpreter.execfile(script.getAbsolutePath());
		}
	}
}
