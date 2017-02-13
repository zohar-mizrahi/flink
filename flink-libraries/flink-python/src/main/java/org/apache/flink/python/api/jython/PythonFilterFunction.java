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
