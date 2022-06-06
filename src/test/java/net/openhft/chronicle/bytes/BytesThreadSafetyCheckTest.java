/*
 * Copyright (c) 2016-2022 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.bytes;

import net.openhft.chronicle.core.io.AbstractReferenceCounted;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

final class BytesThreadSafetyCheckTest {

    @ParameterizedTest(name = "{index}: ({0})")
    @MethodSource("bytesToTest")
    void testThreadOwnershipByConstructor(String classSimpleName, Bytes<?> bytes) {
        // We should receive "untainted" Bytes that are not touched by any thread.
        // Here, we make sure we can do an operation (i.e. clear) with a
        // thread that is distinct to the one created it.

        System.out.println( bytes.isImmutableEmptyByteStore());

        CapturingThread thread = runInAnotherThread(bytes::clear, classSimpleName);
        assertTrue(thread.isCompletedExceptionally());
    }

    @ParameterizedTest(name = "{index}: ({0})")
    @MethodSource("bytesToTest")
    void clear(String classSimpleName, Bytes<?> bytes) {
        releaseThreadOwnership(bytes);
        doInTwoThreads(bytes, Bytes::clear);
    }

    void doInTwoThreads(Bytes<?> bytes, Consumer<Bytes<?>> operator) {
        final Runnable action = () -> operator.accept(bytes);
        final CapturingThread t1 = runInAnotherThread(action, "First thread");
        final CapturingThread t2 = runInAnotherThread(action, "Second thread");

        final boolean oneFailed = t1.isCompletedExceptionally() ^ t2.isCompletedExceptionally();

        assertTrue(oneFailed, "Exactly one of the two threads should fail but this was not the case: " + t1.isCompletedExceptionally() + ", " + t2.isCompletedExceptionally());

/*        System.out.println("f1.toString() = " + t1.toString());
        System.out.println("f2.toString() = " + t2.toString());*/
    }

    static CapturingThread runInAnotherThread(Runnable target, String name) {
        final CapturingThread thread = new CapturingThread(target, name);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException ie) {
            fail(ie);
        }
        return thread;
    }

    final static class CapturingThread extends Thread {

        private volatile Exception exception;

        public CapturingThread(Runnable target, String name) {
            super(target, name);
        }

        @Override
        public void run() {
            try {
                super.run();
            } catch (Exception exception) {
                this.exception = exception;
            }
        }

        public Exception exception() {
            return exception;
        }

        public boolean isCompletedExceptionally() {
            return exception != null;
        }

    }

    static void releaseThreadOwnership(final Bytes<?> bytes) {
        ((AbstractReferenceCounted) bytes).releaseThreadOwnership();
    }

    static Stream<Arguments> bytesToTest() {
        return Stream.of(
                        Bytes.allocateDirect(10),
                        // Thread-tainted Bytes.wrapForRead(new byte[10])
                        Bytes.allocateElasticOnHeap(10)
                )
                .map(b -> Arguments.of(b.getClass().getSimpleName(), b));
    }


}