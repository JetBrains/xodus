/*
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
package jetbrains.vectoriadb.index;

public enum Distance {
    L2 {
        @Override
        public DistanceFunction searchDistanceFunction() {
            return L2DistanceFunction.INSTANCE;
        }

        @Override
        public DistanceFunction buildDistanceFunction() {
            return L2DistanceFunction.INSTANCE;
        }

        @Override
        public Quantizer quantizer() {
            return new L2PQQuantizer();
        }
    },
    DOT {
        @Override
        public DistanceFunction searchDistanceFunction() {
            return DotDistanceFunction.INSTANCE;
        }

        @Override
        public DistanceFunction buildDistanceFunction() {
            return DotDistanceFunction.INSTANCE;
        }

        @Override
        public Quantizer quantizer() {
            return new L2PQQuantizer();
        }
    },
    COSINE {
        @Override
        public DistanceFunction searchDistanceFunction() {
            return CosineDistanceFunction.INSTANCE;
        }

        @Override
        public DistanceFunction buildDistanceFunction() {
            return CosineDistanceFunction.INSTANCE;
        }

        @Override
        public Quantizer quantizer() {
            return new L2PQQuantizer();
        }
    };

    public abstract DistanceFunction searchDistanceFunction();

    public abstract DistanceFunction buildDistanceFunction();

    public abstract Quantizer quantizer();
}
