/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.vfs;

import jetbrains.exodus.TestFor;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

@SuppressWarnings({"HardcodedLineSeparator"})
public class VfsStreamsTests extends VfsTestsBase {

    private static final String HOEGAARDEN = "hoegaarden";
    private static final String RENAT_GILFANOV =
        "�?очью зде�?ь человек лежал, гл�?д�? в неба тьму,\n" +
            "Вперив глаза в от�?ут�?твие чего бы то ни было.\n" +
            "Ра�?�?е�?нна�? улыбка раздвигала губы ему,\n" +
            "Потом на�?тупил прибой, но воды ни прибыло.\n" +
            "От жёлтой поло�?ки �?вета, крадущей�?�? �? корабл�?\n" +
            "По чёрному дёгтю волн, от мерного их брожени�?\n" +
            "Рыбы впадали в тран�?. И �?еверный ветер, как конопл�?,\n" +
            "Бродил по извилинам волн, во�?пал�?�? воображение.\n" +
            "Лежащий гл�?дел в то ме�?то, откуда бывает �?нег,\n" +
            "Где, обозначенный кра�?ной точкою, медленно двигал�?�? к ката�?трофе\n" +
            "Самолёт, в котором �?овершенно чужой ему человек,\n" +
            "Подозвав �?тюарде�?�?у, �? улыбкой заказывал �?ебе кофе…\n\n" +

            "Может быть, обитатель звёзд\n" +
            "гл�?д�?щий на на�? �? небе�?,\n" +
            "учует тепло наших гнезд.\n" +
            "�?ам нельз�? обойти�?�? без.\n" +
            "�?лое �?олнце кра�?ит\n" +
            "рамы оконной метр.\n" +
            "Дети пьют про�?токвашу,\n" +
            "бо�?�?ь выходить на ветр.\n" +
            "Гл�?д�?, как вечер играет\n" +
            "заката кра�?ным м�?чом,\n" +
            "�?тарый отец умирает,\n" +
            "вжавши�?ь в по�?тель плечом.\n" +
            "Пока гоношит�?�? ветер,\n" +
            "и вку�? про�?токваши ки�?л,\n" +
            "он прикрывает веки\n" +
            "и думает �?вою мы�?ль.\n" +
            "�? мы�?ль его незаметна\n" +
            "и также тиха, как он.\n" +
            "Слабей завывань�? ветра\n" +
            "и дребезжань�? окон.\n" +
            "Мы�?ль не имеет приметы,\n" +
            "лишь им�? длиной в �?троку.\n" +
            "�?м�? то \"�?мерть\", и �?то\n" +
            "нравит�?�? �?тарику.";

    @Test
    public void writeRead() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write(HOEGAARDEN.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals(HOEGAARDEN, streamAsString(inputStream));
        inputStream.close();
        txn.abort();
    }

    @Test
    public void writeRead2() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write(HOEGAARDEN.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals(HOEGAARDEN, streamAsString(inputStream));
        inputStream.close();
        txn.commit();
    }

    @Test
    public void writeRead3() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.writeFile(txn, file0);
        outputStream.write(0);
        outputStream.close();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals(0, inputStream.read());
        inputStream.close();
        txn.commit();
    }

    @Test
    public void writeRead4() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.writeFile(txn, file0);
        final int count = 0x10000;
        outputStream.write(new byte[count]);
        outputStream.close();
        final InputStream inputStream = vfs.readFile(txn, file0);
        for (int i = 0; i < count; ++i) {
            Assert.assertEquals(0, inputStream.read());
        }
        inputStream.close();
        txn.commit();
    }

    @Test
    public void writeRead5() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.writeFile(txn, file0);
        final int count = 0x10000;
        outputStream.write(new byte[count]);
        outputStream.close();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals(count, inputStream.read(new byte[100000], 0, 100000));
        Assert.assertEquals(-1, inputStream.read());
        inputStream.close();
        txn.commit();
    }

    @Test
    public void writeLength() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.appendFile(txn, file0);
        final byte[] bytes = HOEGAARDEN.getBytes(StandardCharsets.UTF_8);
        outputStream.write(bytes);
        outputStream.close();
        txn.commit();
        env.executeInReadonlyTransaction(txn1 -> {
            Assert.assertEquals(bytes.length, vfs.getFileLength(txn1, file0));
            // second time to test cached value
            Assert.assertEquals(bytes.length, vfs.getFileLength(txn1, file0));
        });
    }

    @Test
    public void writeReadTrivial() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.writeFile(txn, file0);
        outputStream.close();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals(-1, inputStream.read());
        inputStream.close();
        txn.commit();
    }

    @Test
    public void writeRandomAccessRead() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write((HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + HOEGAARDEN).getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        InputStream inputStream = vfs.readFile(txn, file0, 0);
        Assert.assertEquals(HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + HOEGAARDEN, streamAsString(inputStream));
        inputStream.close();
        inputStream = vfs.readFile(txn, file0, 10);
        Assert.assertEquals(HOEGAARDEN + HOEGAARDEN + HOEGAARDEN, streamAsString(inputStream));
        inputStream.close();
        inputStream = vfs.readFile(txn, file0, 20);
        Assert.assertEquals(HOEGAARDEN + HOEGAARDEN, streamAsString(inputStream));
        inputStream.close();
        inputStream = vfs.readFile(txn, file0, 30);
        Assert.assertEquals(HOEGAARDEN, streamAsString(inputStream));
        inputStream.close();
        txn.abort();
    }

    @Test
    public void writeOverwriteRead() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write(HOEGAARDEN.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        outputStream = vfs.writeFile(txn, file0);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals('x' + HOEGAARDEN.substring(1), streamAsString(inputStream));
        inputStream.close();
        txn.abort();
    }

    @Test
    public void writeOverwriteRead2() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write((HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + HOEGAARDEN).getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        outputStream = vfs.writeFile(txn, file0);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals('x' + HOEGAARDEN.substring(1) +
            'x' + HOEGAARDEN.substring(1) + 'x' + HOEGAARDEN.substring(1) +
            'x' + HOEGAARDEN.substring(1), streamAsString(inputStream));
        inputStream.close();
        txn.abort();
    }

    @Test
    public void writeOverwriteAppendRead() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write((HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + HOEGAARDEN).getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        outputStream = vfs.writeFile(txn, file0);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.write(HOEGAARDEN.substring(1).getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        outputStream = vfs.appendFile(txn, file0);
        outputStream.write(HOEGAARDEN.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals('x' + HOEGAARDEN.substring(1) +
            'x' + HOEGAARDEN.substring(1) + 'x' + HOEGAARDEN.substring(1) +
            'x' + HOEGAARDEN.substring(1) + HOEGAARDEN, streamAsString(inputStream));
        inputStream.close();
        txn.abort();
    }

    @Test
    public void writeNegativePosition() {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        TestUtil.runWithExpectedException(() -> vfs.writeFile(txn, file0, -1), IllegalArgumentException.class);
        txn.commit();
    }

    @Test
    public void writeRandomAccessOverwriteRead() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write((HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + HOEGAARDEN).getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        Assert.assertEquals(40, vfs.getFileLength(txn, file0));
        outputStream = vfs.writeFile(txn, file0, 0);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        Assert.assertEquals(40, vfs.getFileLength(txn, file0));
        outputStream = vfs.writeFile(txn, file0, 10);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        Assert.assertEquals(40, vfs.getFileLength(txn, file0));
        outputStream = vfs.writeFile(txn, file0, 20);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        Assert.assertEquals(40, vfs.getFileLength(txn, file0));
        outputStream = vfs.writeFile(txn, file0, 30);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        Assert.assertEquals(40, vfs.getFileLength(txn, file0));
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals('x' + HOEGAARDEN.substring(1) +
            'x' + HOEGAARDEN.substring(1) + 'x' + HOEGAARDEN.substring(1) +
            'x' + HOEGAARDEN.substring(1), streamAsString(inputStream));
        inputStream.close();
        txn.abort();
    }

    @Test
    public void writeRandomAccessOverwriteRead2() throws IOException {
        final Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write((HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + HOEGAARDEN).getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        outputStream = vfs.writeFile(txn, file0, 1000000);
        outputStream.write("x".getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file0);
        Assert.assertEquals(HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + HOEGAARDEN + 'x', streamAsString(inputStream));
        inputStream.close();
        txn.abort();
    }

    @Test
    public void writeReadLinearStrategy() throws IOException {
        testWriteReadWithStrategy(ClusteringStrategy.LINEAR);
    }

    @Test
    public void writeReadLinearStrategy2() throws IOException {
        testWriteReadWithStrategy(new ClusteringStrategy.LinearClusteringStrategy(16));
    }

    @Test
    public void writeReadQuadraticStrategy() throws IOException {
        testWriteReadWithStrategy(ClusteringStrategy.QUADRATIC);
    }

    @Test
    public void writeReadExponentialStrategy() throws IOException {
        testWriteReadWithStrategy(ClusteringStrategy.EXPONENTIAL);
    }

    @Test
    public void writeOverwriteReadLinearStrategy() throws IOException {
        testWriteOverwriteReadWithStrategy(ClusteringStrategy.LINEAR);
    }

    @Test
    public void writeOverwriteReadQuadraticStrategy() throws IOException {
        testWriteOverwriteReadWithStrategy(ClusteringStrategy.QUADRATIC);
    }

    @Test
    public void writeOverwriteReadExponentialStrategy() throws IOException {
        testWriteOverwriteReadWithStrategy(ClusteringStrategy.EXPONENTIAL);
    }

    @Test
    public void testTimeFieldsOfFiles() throws IOException, InterruptedException {
        final long start = System.currentTimeMillis();
        Thread.sleep(20);
        final Transaction txn = env.beginTransaction();
        File file0 = vfs.createFile(txn, "file0");
        txn.flush();
        Thread.sleep(20);
        final OutputStream outputStream = vfs.appendFile(txn, file0);
        outputStream.write(RENAT_GILFANOV.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        txn.flush();
        file0 = vfs.openFile(txn, "file0", false);
        Assert.assertNotNull(file0);
        Assert.assertTrue(file0.getCreated() > start);
        Assert.assertTrue(file0.getCreated() < file0.getLastModified());
        txn.abort();
    }

    @Test
    public void testFileLength() throws IOException {
        final Transaction txn = env.beginTransaction();
        vfs.createFile(txn, "file0");
        txn.flush();
        for (int i = 0; i < 300; ++i) {
            final File file0 = vfs.openFile(txn, "file0", false);
            Assert.assertNotNull(file0);
            Assert.assertEquals(i, vfs.getFileLength(txn, file0));
            final OutputStream outputStream = vfs.appendFile(txn, file0);
            outputStream.write(i);
            outputStream.close();
            txn.flush();
        }
        txn.abort();
    }

    @Test
    public void testWriteAndSeek() throws IOException {
        Transaction txn = env.beginTransaction();
        final File file = vfs.createFile(txn, "file0");
        OutputStream outputStream;
        for (int i = 0; i < 4; ++i) {
            outputStream = vfs.writeFile(txn, file, i);
            outputStream.write(HOEGAARDEN.getBytes(StandardCharsets.UTF_8));
            outputStream.close();
        }
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file);
        Assert.assertEquals("hhh" + HOEGAARDEN, streamAsString(inputStream));
        txn.abort();
    }

    @Test
    @TestFor(issue = "XD-624")
    public void testWriteAndSeek2() throws IOException {
        Transaction txn = env.beginTransaction();
        vfs.getConfig().setClusteringStrategy(new ClusteringStrategy.LinearClusteringStrategy(8));
        final File file = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.writeFile(txn, file);
        final byte[] bytes = HOEGAARDEN.getBytes(StandardCharsets.UTF_8);
        outputStream.write(bytes);
        outputStream.close();
        outputStream = vfs.writeFile(txn, file, bytes.length);
        outputStream.write(bytes);
        outputStream.close();
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file);
        Assert.assertEquals(HOEGAARDEN + HOEGAARDEN, streamAsString(inputStream));
        txn.abort();
    }

    @Test
    @TestFor(issue = "XD-624")
    public void testWriteAndSeek3() throws IOException {
        Transaction txn = env.beginTransaction();
        vfs.getConfig().setClusteringStrategy(new ClusteringStrategy.LinearClusteringStrategy(8));
        final File file = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.writeFile(txn, file);
        final byte[] bytes = HOEGAARDEN.getBytes(StandardCharsets.UTF_8);
        outputStream.write(bytes);
        outputStream.close();
        outputStream = vfs.appendFile(txn, file);
        outputStream.write(bytes);
        outputStream.close();
        txn.flush();
        final InputStream inputStream = vfs.readFile(txn, file);
        Assert.assertEquals(HOEGAARDEN + HOEGAARDEN, streamAsString(inputStream));
        txn.abort();
    }

    private void testWriteReadWithStrategy(@NotNull final ClusteringStrategy strategy) throws IOException {
        vfs.shutdown();
        final VfsConfig config = new VfsConfig();
        config.setClusteringStrategy(strategy);
        vfs = new VirtualFileSystem(getEnvironment(), config);
        final Transaction txn = env.beginTransaction();
        final byte[] bytes = RENAT_GILFANOV.getBytes(StandardCharsets.UTF_8);
        final File file0 = vfs.createFile(txn, "file0");
        final OutputStream outputStream0 = vfs.appendFile(txn, file0);
        outputStream0.write(bytes);
        outputStream0.close();
        final File file1 = vfs.createFile(txn, "file1");
        final OutputStream outputStream1 = vfs.appendFile(txn, file1);
        outputStream1.write(bytes);
        outputStream1.close();
        final InputStream inputStream0 = vfs.readFile(txn, file0);
        Assert.assertEquals(RENAT_GILFANOV, streamAsString(inputStream0));
        inputStream0.close();
        final InputStream inputStream1 = vfs.readFile(txn, file1);
        Assert.assertEquals(RENAT_GILFANOV, streamAsString(inputStream1));
        inputStream1.close();
        Assert.assertEquals(bytes.length, vfs.getFileLength(txn, file0));
        Assert.assertEquals(bytes.length, vfs.getFileLength(txn, file1));
        txn.commit();
    }

    private void testWriteOverwriteReadWithStrategy(@NotNull final ClusteringStrategy strategy) throws IOException {
        vfs.shutdown();
        final VfsConfig config = new VfsConfig();
        config.setClusteringStrategy(strategy);
        vfs = new VirtualFileSystem(getEnvironment(), config);
        Transaction txn = env.beginTransaction();
        final File file0 = vfs.createFile(txn, "file0");
        OutputStream outputStream = vfs.writeFile(txn, file0);
        outputStream.write(HOEGAARDEN.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        InputStream inputStream = vfs.readFile(txn, file0);
        String actualRead = streamAsString(inputStream);
        Assert.assertEquals(HOEGAARDEN, actualRead);
        inputStream.close();
        txn.commit();
        txn = env.beginTransaction();
        outputStream = vfs.writeFile(txn, file0);
        outputStream.write(RENAT_GILFANOV.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        inputStream = vfs.readFile(txn, file0);
        actualRead = streamAsString(inputStream);
        Assert.assertEquals(RENAT_GILFANOV, actualRead);
        inputStream.close();
        txn.commit();
    }

    private static String streamAsString(@NotNull final InputStream inputStream) throws IOException {
        final InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        final StringBuilder result = new StringBuilder();
        int c;
        while ((c = streamReader.read()) != -1) {
            result.append((char) c);
        }
        return result.toString();
    }
}
