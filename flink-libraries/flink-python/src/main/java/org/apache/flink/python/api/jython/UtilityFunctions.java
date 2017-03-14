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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.python.core.Py;
import org.python.core.PyObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.python.util.PythonInterpreter;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

// import static org.python.core.PySystemState.registry;

public class UtilityFunctions {
	private static boolean jythonInitialized = false;

	private UtilityFunctions() {
	}

	public static class SerializerMap<IN> implements MapFunction<IN, PyObject> {
		private static final long serialVersionUID = 1582769662549499373L;

		@Override
		public PyObject map(IN value) throws Exception {
			return Py.java2py(value);
		}
	}

	public static PyObject adapt(Object o) {
		if (o instanceof PyObject) {
			return (PyObject)o;
		}
		return  Py.java2py(o);
	}

	public static synchronized Object smartFunctionDeserialization(RuntimeContext runtimeCtx, byte[] serFun) throws IOException, ClassNotFoundException {
		File path = runtimeCtx.getDistributedCache().getFile(PythonEnvironmentConfig.FLINK_PYTHON_DC_ID);
		System.out.println(String.format("Cached Path: %s", path.getParent()));
		if (!jythonInitialized) {
			/**
			 * We have to initialise the jython interpreter before any call to jython related functions,
			 * otherwise the default initialisation would be called and thus the proper python path
			 * would not be set.
			 */
			// File path = runtimeCtx.getDistributedCache().getFile(PythonEnvironmentConfig.FLINK_PYTHON_DC_ID);
			UtilityFunctions.initPythonInterpreter(new File(path.getPath() + File.separator + "plan.py"));
			jythonInitialized = true;
		}

		try {
			return SerializationUtils.deserializeObject(serFun);
		} catch (Exception e) {
			// File path = runtimeCtx.getDistributedCache().getFile(PythonEnvironmentConfig.FLINK_PYTHON_DC_ID);
			// PySystemState.add_classdir(path.getPath());
			UtilityFunctions.initPythonInterpreter(new File(path.getPath() + File.separator + "plan.py"));
			return SerializationUtils.deserializeObject(serFun);
		}
	}

	public static void initPythonInterpreter(File scriptFullPath) {
		Properties postProperties = new Properties();
		postProperties.put("python.path", scriptFullPath.getParent());
		PythonInterpreter.initialize(System.getProperties(), postProperties, new String[] {""});
		// registry.putAll(postProperties);
		try (PythonInterpreter interpreter = new PythonInterpreter()) {
			interpreter.setErr(System.err);
			interpreter.setOut(System.out);
			interpreter.execfile(scriptFullPath.getAbsolutePath());
		}
	}
}
