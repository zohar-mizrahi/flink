/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.util.CollectionUtil;

import java.util.Collection;
import java.util.List;

/**
 * This class holds all state handles for one operator.
 */
@Internal
@VisibleForTesting
public class OperatorStateHandles {

	private final int operatorChainIndex;

	private final Collection<KeyedStateHandle> managedKeyedState;
	private final Collection<KeyedStateHandle> rawKeyedState;
	private final Collection<OperatorStateHandle> managedOperatorState;
	private final Collection<OperatorStateHandle> rawOperatorState;

	public OperatorStateHandles(
			int operatorChainIndex,
			Collection<KeyedStateHandle> managedKeyedState,
			Collection<KeyedStateHandle> rawKeyedState,
			Collection<OperatorStateHandle> managedOperatorState,
			Collection<OperatorStateHandle> rawOperatorState) {

		this.operatorChainIndex = operatorChainIndex;
		this.managedKeyedState = managedKeyedState;
		this.rawKeyedState = rawKeyedState;
		this.managedOperatorState = managedOperatorState;
		this.rawOperatorState = rawOperatorState;
	}

	public Collection<KeyedStateHandle> getManagedKeyedState() {
		return managedKeyedState;
	}

	public Collection<KeyedStateHandle> getRawKeyedState() {
		return rawKeyedState;
	}

	public Collection<OperatorStateHandle> getManagedOperatorState() {
		return managedOperatorState;
	}

	public Collection<OperatorStateHandle> getRawOperatorState() {
		return rawOperatorState;
	}

	public int getOperatorChainIndex() {
		return operatorChainIndex;
	}

	private static <T> T getSafeItemAtIndexOrNull(List<T> list, int idx) {
		return CollectionUtil.isNullOrEmpty(list) ? null : list.get(idx);
	}
}
