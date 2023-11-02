/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.vectoriadb.client;

import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import jetbrains.vectoriadb.service.base.IndexManagerGrpc;
import jetbrains.vectoriadb.service.base.IndexManagerOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public final class VectoriaDBClient {
    private static final Logger logger = LoggerFactory.getLogger(VectoriaDBClient.class);
    private final IndexManagerGrpc.IndexManagerBlockingStub indexManagerBlockingStub;
    private final IndexManagerGrpc.IndexManagerStub indexManagerAsyncStub;

    public VectoriaDBClient(String host) {
        this(host, 9090);
    }

    public VectoriaDBClient(String host, int port) {
        var channel = ManagedChannelBuilder.forAddress(host, port).build();
        this.indexManagerBlockingStub = IndexManagerGrpc.newBlockingStub(channel);
        this.indexManagerAsyncStub = IndexManagerGrpc.newStub(channel);
    }

    public IndexMetadata createIndex(final String indexName, final Distance distance) {
        var builder = IndexManagerOuterClass.CreateIndexRequest.newBuilder();
        builder.setIndexName(indexName);

        switch (distance) {
            case L2:
                builder.setDistance(IndexManagerOuterClass.Distance.L2);
                break;
            case DOT:
                builder.setDistance(IndexManagerOuterClass.Distance.DOT);
                break;
            case COSINE:
                builder.setDistance(IndexManagerOuterClass.Distance.COSINE);
                break;
        }

        var request = builder.build();
        var response = indexManagerBlockingStub.createIndex(request);

        return new IndexMetadata(response.getMaximumConnectionsPerVertex(), response.getMaximumCandidatesReturned(),
                response.getCompressionRatio(), response.getDistanceMultiplier());
    }

    public void buildIndex(final String indexName) {
        var builder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        builder.setIndexName(indexName);

        var request = builder.build();
        //noinspection ResultOfMethodCallIgnored
        indexManagerBlockingStub.buildIndex(request);
    }

    public void dropIndex(final String indexName) {
        var builder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        builder.setIndexName(indexName);

        var request = builder.build();
        //noinspection ResultOfMethodCallIgnored
        indexManagerBlockingStub.dropIndex(request);
    }

    public List<String> listIndexes() {
        var builder = Empty.newBuilder();
        var request = builder.build();

        var response = indexManagerBlockingStub.indexList(request);
        return response.getIndexNamesList();
    }

    IndexState indexState(String indexName) {
        var builder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        builder.setIndexName(indexName);

        var request = builder.build();
        var response = indexManagerBlockingStub.indexState(request);

        return switch (response.getState()) {
            case CREATING -> IndexState.CREATING;
            case CREATED -> IndexState.CREATED;
            case UPLOADING -> IndexState.UPLOADING;
            case UPLOADED -> IndexState.UPLOADED;
            case IN_BUILD_QUEUE -> IndexState.IN_BUILD_QUEUE;
            case BUILDING -> IndexState.BUILDING;
            case BUILT -> IndexState.BUILT;
            case BROKEN -> IndexState.BROKEN;
            default -> throw new IllegalStateException("Unexpected value: " + response.getState());
        };
    }

    public void uploadVectors(final String indexName, final Iterator<float[]> vectors) {
        var error = new Throwable[1];
        var finishedLatch = new CountDownLatch(1);
        var responseObserver = new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty value) {
                //ignore
            }

            @Override
            public void onError(Throwable t) {
                logger.error("Error while uploading vectors", t);
                error[0] = t;
                finishedLatch.countDown();
            }

            @Override
            public void onCompleted() {
                finishedLatch.countDown();
            }
        };

        var requestObserver = indexManagerAsyncStub.uploadData(responseObserver);
        try {
            while (vectors.hasNext()) {
                var vector = vectors.next();
                var builder = IndexManagerOuterClass.UploadDataRequest.newBuilder();
                builder.setIndexName(indexName);

                for (var value : vector) {
                    builder.addVectorComponents(value);
                }

                var request = builder.build();
                requestObserver.onNext(request);

                if (finishedLatch.getCount() == 0) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }

        responseObserver.onCompleted();
        try {
            finishedLatch.await();
        } catch (InterruptedException e) {
            logger.error("Error while uploading vectors", e);
            Thread.currentThread().interrupt();
        }

        if (error[0] != null) {
            logger.error("Error while uploading vectors", error[0]);
            throw new RuntimeException(error[0]);
        }
    }

    public void switchToSearchMode() {
        var builder = Empty.newBuilder();
        var request = builder.build();
        //noinspection ResultOfMethodCallIgnored
        indexManagerBlockingStub.switchToSearchMode(request);
    }

    public void switchToBuildMode() {
        var builder = Empty.newBuilder();
        var request = builder.build();
        //noinspection ResultOfMethodCallIgnored
        indexManagerBlockingStub.switchToBuildMode(request);
    }

    public int[] findNearestNeighbours(final String indexName, int k) {
        var builder = IndexManagerOuterClass.FindNearestNeighboursRequest.newBuilder();
        builder.setIndexName(indexName);
        builder.setK(k);

        var request = builder.build();
        var response = indexManagerBlockingStub.findNearestNeighbours(request);

        return response.getIdsList().stream().mapToInt(Integer::intValue).toArray();
    }

    public void buildStatus(IndexBuildStatusListener buildStatusListener) {
        var builder = Empty.newBuilder();
        var request = builder.build();

        try (var cancellation = Context.current().withCancellation()) {
            var response = indexManagerBlockingStub.buildStatus(request);
            while (response.hasNext()) {
                var status = response.next();
                var indexName = status.getIndexName();

                var phasesResponse = status.getPhasesList();
                var phases = new ArrayList<IndexBuildStatusListener.Phase>(phasesResponse.size());

                for (var phase : phasesResponse) {
                    var phaseName = phase.getName();
                    var progress = phase.getCompletionPercentage();
                    var parameters = phase.getParametersList().toArray(new String[0]);

                    phases.add(new IndexBuildStatusListener.Phase(phaseName, progress, parameters));
                }

                if (!buildStatusListener.onIndexBuildStatusUpdate(indexName, phases)) {
                    cancellation.cancel(new InterruptedException("Cancelled by build status listener"));
                    break;
                }
            }
        }
    }
}
