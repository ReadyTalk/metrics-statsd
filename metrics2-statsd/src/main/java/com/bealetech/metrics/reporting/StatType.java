/**
 * Copyright (C) 2012-2013 Sean Laurent
 * Copyright (C) 2013 Michael Keesey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.bealetech.metrics.reporting;

public enum StatType {
	COUNTER("c"), TIMER("ms"), GAUGE("g");

	private final String statsdType;

	private StatType(String statsdType) {
		this.statsdType = statsdType;
	}

	public String statsdType() {
		return statsdType;
	}
}
