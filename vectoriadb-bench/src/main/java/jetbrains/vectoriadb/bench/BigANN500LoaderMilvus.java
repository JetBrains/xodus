package jetbrains.vectoriadb.bench;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BigANN500LoaderMilvus {
    public static final int VECTOR_DIMENSIONS = 128;

    public static final int VECTORS_COUNT = 500_000_000;

    public static void main(String[] args) throws Exception {
        var benchPathStr = System.getProperty("bench.path");
        Path benchPath;

        benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        var baseArchiveName = "bigann_base.bvecs.gz";
        var dataFileName = "bigann_base.bvecs";
        var dataFilePath = benchPath.resolve(dataFileName);

        if (!Files.exists(dataFilePath) || Files.size(dataFilePath) == 0) {
            var baseArchivePath = BenchUtils.downloadBenchFile(benchPath, baseArchiveName);
            BenchUtils.extractGzArchive(dataFilePath, baseArchivePath);
        }

        System.out.printf("%d data vectors loaded with dimension %d for BigANN index, ...%n",
                VECTORS_COUNT, VECTOR_DIMENSIONS);

        var ts1 = System.nanoTime();
        final MilvusServiceClient milvusClient = new MilvusServiceClient(
                ConnectParam.newBuilder()
                        .withHost("localhost")
                        .withPort(19530)
                        .build()
        );

        FieldType idField = FieldType.newBuilder()
                .withName("id")
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(false)
                .build();
        FieldType vectorField = FieldType.newBuilder()
                .withName("vector")
                .withDataType(DataType.FloatVector)
                .withDimension(VECTOR_DIMENSIONS)
                .build();
        CreateCollectionParam createCollectionReq = CreateCollectionParam.newBuilder()
                .withCollectionName("test")
                .addFieldType(idField)
                .addFieldType(vectorField)
                .build();
        milvusClient.createCollection(createCollectionReq);

        var recordSize = Integer.BYTES + VECTOR_DIMENSIONS;
        System.out.println("Loading vectors into Milvus...");

        try (var channel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
            var buffer =
                    ByteBuffer.allocate(
                            (64 * 1024 * 1024 / recordSize) * recordSize).order(ByteOrder.LITTLE_ENDIAN);

            while (buffer.remaining() > 0) {
                channel.read(buffer);
            }
            buffer.rewind();

            for (long i = 0; i < VECTORS_COUNT; i++) {
                if (buffer.remaining() == 0) {
                    buffer.rewind();

                    while (buffer.remaining() > 0) {
                        var r = channel.read(buffer);
                        if (r == -1) {
                            break;
                        }
                    }
                    buffer.clear();
                }

                var dimensions = buffer.getInt();
                if (dimensions != VECTOR_DIMENSIONS) {
                    throw new RuntimeException("Vector dimensions mismatch : " +
                            dimensions + " vs " + VECTOR_DIMENSIONS);
                }

                var vector = new float[VECTOR_DIMENSIONS];
                for (int j = 0; j < VECTOR_DIMENSIONS; j++) {
                    vector[j] = buffer.get();
                }

                List<InsertParam.Field> fields = new ArrayList<>();
                fields.add(new InsertParam.Field("id", new ArrayList<>(List.of(i))));
                fields.add(new InsertParam.Field("vector", new ArrayList<>(List.of(vector))));

                InsertParam insertParam = InsertParam.newBuilder()
                        .withCollectionName("test")
                        .withFields(fields)
                        .build();
                milvusClient.insert(insertParam);

                if ((i + 1) % 1_000_000 == 0) {
                    System.out.printf("%d vectors loaded.%n", i + 1);
                }
            }
        }

        milvusClient.flush(FlushParam.newBuilder().withSyncFlush(true).build());
        var ts2 = System.nanoTime();

        System.out.printf("Data loaded in %d ms.%n", (ts2 - ts1) / 1000000);
        System.out.println("Building index...");


        ts1 = System.nanoTime();
        milvusClient.createIndex(
                CreateIndexParam.newBuilder()
                        .withCollectionName("test")
                        .withFieldName("vector")
                        .withIndexType(IndexType.DISKANN)
                        .withMetricType(MetricType.IP)
                        .withSyncMode(Boolean.TRUE)
                        .build()
        );
        ts2 = System.nanoTime();
        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

        milvusClient.close();

    }
}
