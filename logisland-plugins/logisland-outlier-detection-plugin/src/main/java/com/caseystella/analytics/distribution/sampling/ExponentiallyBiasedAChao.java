/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.caseystella.analytics.distribution.sampling;

import java.util.Random;

/**
 * Keeps an exponentially weighted sample with specified bias parameter
 * N.B. The current period is advanced explicitly.
 * For example,
 */
public class ExponentiallyBiasedAChao<T> extends AChao<T> {
    private final double bias;

    public ExponentiallyBiasedAChao(int capacity, double bias, Random random) {
        super(capacity, random);
        assert (bias >= 0 && bias < 1);
        this.bias = bias;
    }

    public ExponentiallyBiasedAChao(int capacity, double bias) {
        super(capacity);
        assert (bias >= 0 && bias < 1);
        this.bias = bias;
    }

    public void advancePeriod() {
        advancePeriod(1);
    }

    public void advancePeriod(int numPeriods) {
        runningCount *= Math.pow(1 - bias, numPeriods);
    }

    public void insert(T ele) {
        insert(ele, 1);
    }
}
