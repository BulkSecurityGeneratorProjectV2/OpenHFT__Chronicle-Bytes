/*
 * Copyright (c) 2016-2022 chronicle.software
 *
 *     https://chronicle.software
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteCheckSumTest extends BytesTestCommon {
    @Test
    public void test() {
        Bytes<?> bytes = Bytes.allocateDirect(32);
        doTest(bytes);
        bytes.releaseLast();
    }

    @Test
    public void testHeap() {
        Bytes<?> bytes = Bytes.allocateElasticOnHeap(32);
        doTest(bytes);
        bytes.releaseLast();
    }

    private void doTest(Bytes<?> bytes) {
        bytes.append("abcdef");
        assertEquals(('a' + 'b' + 'c' + 'd' + 'e' + 'f') & 0xff, bytes.byteCheckSum());
        assertEquals(('b' + 'c' + 'd' + 'e' + 'f') & 0xff, bytes.byteCheckSum(1, 6));
        assertEquals(('b' + 'c' + 'd') & 0xff, bytes.byteCheckSum(1, 4));
        assertEquals(('c' + 'd') & 0xff, bytes.byteCheckSum(2, 4));
        assertEquals(('c') & 0xff, bytes.byteCheckSum(2, 3));
    }
}
