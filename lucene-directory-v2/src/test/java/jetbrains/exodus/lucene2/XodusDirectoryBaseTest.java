package jetbrains.exodus.lucene2;

import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class XodusDirectoryBaseTest extends BaseDirectoryTestCase {

    @Test
    public void testDurableDir() throws IOException, InterruptedException {

        final var fileCount = RandomUtils.nextInt(10, 1000);
        final var content = IntStream.range(0, fileCount)
                .boxed()
                .collect(Collectors.toMap(
                        i -> "file_" + i,
                        i -> RandomUtils.nextBytes(RandomUtils.nextInt(100, 100000))
                ));

        final var fileDir = createTempDir("testDurableDir");
        try (Directory dir = getDirectory(fileDir)) {
            for (Map.Entry<String, byte[]> e : content.entrySet()) {
                final var fileName = e.getKey();
                final var bytes = e.getValue();
                try (var o = dir.createOutput(fileName, newIOContext(random()))) {
                    o.writeBytes(bytes, bytes.length);
                }
            }
        }

        try (Directory dir = getDirectory(fileDir)) {
            final var fileNames = Set.of(dir.listAll());

            assertEquals(fileNames, content.keySet());

            for (String fileName : content.keySet()) {
                final var expectedBytes = content.get(fileName);
                try (var i = dir.openInput(fileName, newIOContext(random()))) {

                    assertEquals(expectedBytes.length, i.length());

                    final var actualBytes = new byte[expectedBytes.length];
                    i.readBytes(actualBytes, 0, actualBytes.length);

                    assertArrayEquals(expectedBytes, actualBytes);
                }
            }
        }
    }
}
