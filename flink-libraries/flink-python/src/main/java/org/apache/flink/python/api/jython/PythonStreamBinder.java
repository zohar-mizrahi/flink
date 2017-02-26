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

import org.apache.flink.api.java.utils.ParameterTool;
import org.python.util.PythonInterpreter;

import java.io.File;

public class PythonStreamBinder {
	final private static String defaultPythonScriptName = "run_all_tests.py";

	private PythonStreamBinder() {
	}

	public static void main(String[] args) throws Exception {

		File script = getPythonScriptPath(args);
		if (isFirstArgAnOption(args)) {
			args = prepend(args, script.getAbsolutePath());  // First argument in sys.argv is the script full path
		}

		PythonInterpreter.initialize(System.getProperties(), System.getProperties(), args);
		try (PythonInterpreter interpreter = new PythonInterpreter()) {
			interpreter.setErr(System.err);
			interpreter.setOut(System.out);

			interpreter.execfile(script.getAbsolutePath());
		}
	}

	private static File getPythonScriptPath(String[] args) {
		File script;
		if (args.length > 0) {
			if (isFirstArgAnOption(args)) {
				final ParameterTool params = ParameterTool.fromArgs(args);
				String scriptResource = params.get("script", defaultPythonScriptName);
				script = new File(PythonStreamBinder.class.getClassLoader().getResource(scriptResource).getFile());
			} else {
				script = new File(args[0]);
			}
		} else {
			script = new File(PythonStreamBinder.class.getClassLoader().getResource(defaultPythonScriptName).getFile());
		}
		return script;
	}

	private static boolean isFirstArgAnOption(String[] args) {
		return args.length > 0 && args[0].startsWith("-");
	}

	private static String[] prepend(String[] a, String prepended) {
		String[] c = new String[a.length+1];
		c[0] = prepended;
		System.arraycopy(a, 0, c, 1, a.length);
		return c;
	}

}
