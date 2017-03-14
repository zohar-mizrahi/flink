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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.filecache.FileCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Random;


public class PythonStreamBinder {
	private static final Random r = new Random();
	public static final String FLINK_PYTHON_FILE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "flink_streaming_plan";


	private PythonStreamBinder() {
	}

	/**
	 * Entry point for the execution of a python streaming task.
	 *
	 * @param args <pathToScript> [parameter1]..[parameterX]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		PythonStreamBinder binder = new PythonStreamBinder();
		binder.runPlan(args);
	}

	private void runPlan(String[] args) throws Exception {
		File script;
		if (args.length < 1) {
			System.out.println("Usage: prog <pathToScript> [<pathToPackage1> .. [<pathToPackageX]] - [parameter1]..[parameterX]");
			return;
		}
		else {
			script = new File(args[0]);
			if ((!script.exists()) || (!script.isFile()))
			{
				throw new FileNotFoundException("Could not find file: " + args[0]);
			}
		}

		int split = 0;
		for (int x = 0; x < args.length; x++) {
			if (args[x].compareTo("-") == 0) {
				split = x;
			}
		}

		GlobalConfiguration.loadConfiguration();

		String tmpPath = FLINK_PYTHON_FILE_PATH + r.nextInt();
		prepareFiles(tmpPath, Arrays.copyOfRange(args, 0, split == 0 ? args.length : split));

		if (split != 0) {
			String[] a = new String[args.length - split];
			a[0] = args[0];
			System.arraycopy(args, split + 1, a, 1, args.length - (split +1));
			args = a;
		} else if (args.length > 1) {
			args = new String[]{args[0]};
		}

		UtilityFunctions.initPythonInterpreter(new File(
			tmpPath + File.separator + PythonEnvironmentConfig.FLINK_PYTHON_PLAN_NAME));
	}

	/**
	 * Copies all files to a common directory (FLINK_PYTHON_FILE_PATH). This allows us to distribute it as one big
	 * package, and resolves PYTHONPATH issues.
	 *
	 * @param filePaths
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private void prepareFiles(String tempFilePath, String... filePaths) throws IOException, URISyntaxException {
		FileCache.clearPath(tempFilePath);
		PythonEnvironmentConfig.pythonTmpCachePath = tempFilePath;

		//plan file
		copyFile(filePaths[0], tempFilePath, PythonEnvironmentConfig.FLINK_PYTHON_PLAN_NAME);

		String parentDir = new File(filePaths[0]).getParent();

		//additional files/folders
		for (int x = 1; x < filePaths.length; x++) {
			String currentParent = (new File(filePaths[x])).getParent();
			if (currentParent != parentDir) {
				filePaths[x] = parentDir + File.separator + filePaths[x];
			}
			copyFile(filePaths[x], tempFilePath, null);
		}
	}

	private void copyFile(String path, String target, String name) throws IOException, URISyntaxException {
		if (path.endsWith(File.separator)) {
			path = path.substring(0, path.length() - 1);
		}
		String identifier = name == null ? path.substring(path.lastIndexOf(File.separator)) : name;
		String tmpFilePath = target + File.separator + identifier;
		FileCache.clearPath(tmpFilePath);
		Path p = new Path(path);
		FileCache.copy(p.makeQualified(FileSystem.get(p.toUri())), new Path(tmpFilePath), true);
	}

}
