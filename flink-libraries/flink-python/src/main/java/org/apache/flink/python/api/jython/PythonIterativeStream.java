package org.apache.flink.python.api.jython;

import org.apache.flink.streaming.api.datastream.IterativeStream;

public class PythonIterativeStream extends PythonSingleOutputStreamOperator {

	public PythonIterativeStream(IterativeStream iterativeStream) {
		super(iterativeStream);
	}

	public PythonDataStream close_with(PythonDataStream pystream) {
		((IterativeStream)this.stream).closeWith(pystream.stream);
		return pystream;
	}
}
