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
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import jetbrains.vectoriadb.service.base.IndexManagerGrpc;
import jetbrains.vectoriadb.service.base.IndexManagerOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;


public final class VectoriaDBClient {
    private static final Logger logger = LoggerFactory.getLogger(VectoriaDBClient.class);
    private final IndexManagerGrpc.IndexManagerBlockingStub indexManagerBlockingStub;
    private final IndexManagerGrpc.IndexManagerStub indexManagerAsyncStub;

    public VectoriaDBClient(String host) {
        this(host, 9090);
    }

    public VectoriaDBClient(String host, int port) {
        var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().
                maxInboundMessageSize(10 * 1024 * 1024).build();

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

    public void triggerIndexBuild(final String indexName) {
        var builder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        builder.setIndexName(indexName);

        var request = builder.build();
        //noinspection ResultOfMethodCallIgnored
        indexManagerBlockingStub.triggerIndexBuild(request);
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

        var response = indexManagerBlockingStub.listIndexes(request);
        return response.getIndexNamesList();
    }

    public IndexState retrieveIndexState(String indexName) {
        var builder = IndexManagerOuterClass.IndexNameRequest.newBuilder();
        builder.setIndexName(indexName);

        var request = builder.build();
        var response = indexManagerBlockingStub.retrieveIndexState(request);

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

    public void uploadVectors(final String indexName, final Iterator<float[]> vectors,
                              @Nullable BiConsumer<Integer, Integer> progressIndicator) {
        uploadVectors(indexName, vectors, VectoriaDBClient::uploadVectorsList, progressIndicator);
    }

    public void uploadVectors(final String indexName, final Iterator<float[]> vectors) {
        uploadVectors(indexName, vectors, VectoriaDBClient::uploadVectorsList, null);
    }

    public void uploadVectors(final String indexName, final float[][] vectors,
                              @Nullable BiConsumer<Integer, Integer> progressIndicator) {
        uploadVectors(indexName, vectors, VectoriaDBClient::uploadVectorsArray, progressIndicator);
    }

    public void uploadVectors(final String indexName, final float[][] vectors) {
        uploadVectors(indexName, vectors, VectoriaDBClient::uploadVectorsArray, null);
    }

    private <T> void uploadVectors(String indexName, T vectors, VectorsUploader<T> vectorsUploader,
                                   @Nullable BiConsumer<Integer, Integer> progressIndicator) {
        var error = new Throwable[1];
        var finishedLatch = new CountDownLatch(1);
        var onReadyHandler = new OnReadyHandler<IndexManagerOuterClass.UploadVectorsRequest>();
        var responseObserver = new ClientResponseObserver<IndexManagerOuterClass.UploadVectorsRequest, Empty>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<IndexManagerOuterClass.UploadVectorsRequest> requestStream) {
                onReadyHandler.init(requestStream);
            }

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

        var requestObserver = indexManagerAsyncStub.uploadVectors(responseObserver);
        try {
            vectorsUploader.uploadVectors(indexName, vectors, requestObserver, finishedLatch, onReadyHandler,
                    progressIndicator);
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }

        if (progressIndicator != null) {
            progressIndicator.accept(-1, -1);
            requestObserver.onCompleted();
            progressIndicator.accept(Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
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

    private static void uploadVectorsList(String indexName, Iterator<float[]> vectors,
                                          StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> requestObserver,
                                          CountDownLatch finishedLatch,
                                          OnReadyHandler<IndexManagerOuterClass.UploadVectorsRequest> onReadyHandler,
                                          @Nullable BiConsumer<Integer, Integer> progressIndicator) {
        var counter = new int[1];
        while (vectors.hasNext()) {
            onReadyHandler.callWhenReady(() -> {
                var vector = vectors.next();
                var builder = IndexManagerOuterClass.UploadVectorsRequest.newBuilder();
                builder.setIndexName(indexName);

                for (var value : vector) {
                    builder.addVectorComponents(value);
                }

                var request = builder.build();
                requestObserver.onNext(request);
                if (progressIndicator != null) {
                    counter[0]++;
                    progressIndicator.accept(counter[0], -1);
                }
            });

            if (finishedLatch.getCount() == 0) {
                break;
            }
        }
    }

    private static void uploadVectorsArray(String indexName, float[][] vectors,
                                           StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> requestObserver,
                                           CountDownLatch finishedLatch,
                                           OnReadyHandler<IndexManagerOuterClass.UploadVectorsRequest> onReadyHandler,
                                           @Nullable BiConsumer<Integer, Integer> progressIndicator) {
        var counter = new int[1];
        for (var vector : vectors) {
            onReadyHandler.callWhenReady(() -> {
                var builder = IndexManagerOuterClass.UploadVectorsRequest.newBuilder();
                builder.setIndexName(indexName);

                for (var value : vector) {
                    builder.addVectorComponents(value);
                }

                var request = builder.build();
                requestObserver.onNext(request);
                if (progressIndicator != null) {
                    counter[0]++;
                    progressIndicator.accept(counter[0], vectors.length);
                }
            });

            if (finishedLatch.getCount() == 0) {
                break;
            }
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

    public int[] findNearestNeighbours(final String indexName, final float[] vector, int k) {
        var builder = IndexManagerOuterClass.FindNearestNeighboursRequest.newBuilder();
        builder.setIndexName(indexName);
        builder.setK(k);

        for (var vectorComponent : vector) {
            builder.addVectorComponents(vectorComponent);
        }

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

    public void buildStatusAsync(IndexBuildStatusListener buildStatusListener) {
        var builder = Empty.newBuilder();
        var request = builder.build();

        try (var cancellation = Context.current().withCancellation()) {
            indexManagerAsyncStub.buildStatus(request, new StreamObserver<>() {
                @Override
                public void onNext(IndexManagerOuterClass.BuildStatusResponse value) {
                    var indexName = value.getIndexName();

                    var phasesResponse = value.getPhasesList();
                    var phases = new ArrayList<IndexBuildStatusListener.Phase>(phasesResponse.size());

                    for (var phase : phasesResponse) {
                        var phaseName = phase.getName();
                        var progress = phase.getCompletionPercentage();
                        var parameters = phase.getParametersList().toArray(new String[0]);

                        phases.add(new IndexBuildStatusListener.Phase(phaseName, progress, parameters));
                    }

                    if (!buildStatusListener.onIndexBuildStatusUpdate(indexName, phases)) {
                        cancellation.cancel(new InterruptedException("Cancelled by build status listener"));
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error("Error while getting build status", t);
                    cancellation.cancel(t);
                }

                @Override
                public void onCompleted() {
                    //ignore
                }
            });
        }
    }

    private interface VectorsUploader<T> {
        void uploadVectors(String indexName, T vectors,
                           StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> requestObserver,
                           CountDownLatch finishedLatch, OnReadyHandler<IndexManagerOuterClass.UploadVectorsRequest> onReadyHandler,
                           @Nullable BiConsumer<Integer, Integer> progressIndicator);
    }

    private static final class OnReadyHandler<T> implements Runnable {
        private ClientCallStreamObserver<T> clientCallStreamObserver;
        private final Lock lock = new ReentrantLock();
        private final Condition readyCondition = lock.newCondition();

        private boolean isReady;

        private OnReadyHandler() {
        }

        private void init(ClientCallStreamObserver<T> clientCallStreamObserver) {
            this.clientCallStreamObserver = clientCallStreamObserver;
            clientCallStreamObserver.disableAutoRequestWithInitial(1);
            clientCallStreamObserver.setOnReadyHandler(this);
        }

        private void callWhenReady(Runnable runnable) {
            lock.lock();
            try {
                isReady = clientCallStreamObserver.isReady();
                while (!isReady) {
                    readyCondition.awaitUninterruptibly();
                }

                runnable.run();
            } finally {
                lock.unlock();
            }

        }

        @Override
        public void run() {
            lock.lock();
            try {
                isReady = clientCallStreamObserver.isReady();
                if (isReady) {
                    readyCondition.signal();
                }
            } finally {
                lock.unlock();
            }

        }
    }
}
