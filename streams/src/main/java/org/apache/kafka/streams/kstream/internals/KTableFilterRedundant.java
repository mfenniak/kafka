/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;

import java.util.Comparator;

class KTableFilterRedundant<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final KTableImpl<K, ?, V> parent;
    private final Comparator<V> comparator;

    private boolean sendOldValues = false;

    public KTableFilterRedundant(KTableImpl<K, ?, V> parent, Comparator<V> comparator) {
        this.parent = parent;
        this.comparator = comparator;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableFilterRedundantProcessor();
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return parent.valueGetterSupplier();
    }

    @Override
    public void enableSendingOldValues() {
        parent.enableSendingOldValues();
        sendOldValues = true;
    }

    private class KTableFilterRedundantProcessor extends AbstractProcessor<K, Change<V>> {
        @Override
        public void process(K key, Change<V> change) {
            V newValue = change.newValue;
            V oldValue = change.oldValue;

            if (comparator.compare(oldValue, newValue) == 0) {
                // redundant update; not necessary to forward
                return;
            }

            context().forward(key, new Change<>(newValue, sendOldValues ? oldValue : null));
        }
    }

    static class DefaultComparator<T> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null && o2 != null) {
                return -1;
            }
            if (o1 != null && o2 == null) {
                return 1;
            }
            if (o1.equals(o2)) {
                return 0;
            }
            // We don't really care about the order, only the equality... but just in-case this implementation is ever
            // used where the order might matter, we at least want to return something stable.
            return System.identityHashCode(o1) > System.identityHashCode(o2) ? 1 : -1;
        }
    }
}
