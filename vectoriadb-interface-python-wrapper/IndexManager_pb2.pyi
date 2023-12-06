from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Distance(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    L2: _ClassVar[Distance]
    DOT: _ClassVar[Distance]
    COSINE: _ClassVar[Distance]

class IndexState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    CREATING: _ClassVar[IndexState]
    CREATED: _ClassVar[IndexState]
    UPLOADING: _ClassVar[IndexState]
    UPLOADED: _ClassVar[IndexState]
    IN_BUILD_QUEUE: _ClassVar[IndexState]
    BUILDING: _ClassVar[IndexState]
    BUILT: _ClassVar[IndexState]
    BROKEN: _ClassVar[IndexState]
L2: Distance
DOT: Distance
COSINE: Distance
CREATING: IndexState
CREATED: IndexState
UPLOADING: IndexState
UPLOADED: IndexState
IN_BUILD_QUEUE: IndexState
BUILDING: IndexState
BUILT: IndexState
BROKEN: IndexState

class FindNearestNeighboursRequest(_message.Message):
    __slots__ = ["index_name", "vector_components", "k"]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    K_FIELD_NUMBER: _ClassVar[int]
    index_name: str
    vector_components: _containers.RepeatedScalarFieldContainer[float]
    k: int
    def __init__(self, index_name: _Optional[str] = ..., vector_components: _Optional[_Iterable[float]] = ..., k: _Optional[int] = ...) -> None: ...

class FindNearestNeighboursResponse(_message.Message):
    __slots__ = ["ids"]
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedCompositeFieldContainer[VectorId]
    def __init__(self, ids: _Optional[_Iterable[_Union[VectorId, _Mapping]]] = ...) -> None: ...

class VectorId(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class UploadVectorsRequest(_message.Message):
    __slots__ = ["index_name", "vector_components", "id"]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    index_name: str
    vector_components: _containers.RepeatedScalarFieldContainer[float]
    id: VectorId
    def __init__(self, index_name: _Optional[str] = ..., vector_components: _Optional[_Iterable[float]] = ..., id: _Optional[_Union[VectorId, _Mapping]] = ...) -> None: ...

class IndexListResponse(_message.Message):
    __slots__ = ["index_names"]
    INDEX_NAMES_FIELD_NUMBER: _ClassVar[int]
    index_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, index_names: _Optional[_Iterable[str]] = ...) -> None: ...

class IndexNameRequest(_message.Message):
    __slots__ = ["index_name"]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    index_name: str
    def __init__(self, index_name: _Optional[str] = ...) -> None: ...

class CreateIndexRequest(_message.Message):
    __slots__ = ["index_name", "distance"]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_FIELD_NUMBER: _ClassVar[int]
    index_name: str
    distance: Distance
    def __init__(self, index_name: _Optional[str] = ..., distance: _Optional[_Union[Distance, str]] = ...) -> None: ...

class CreateIndexResponse(_message.Message):
    __slots__ = ["maximumConnectionsPerVertex", "maximumCandidatesReturned", "compressionRatio", "distanceMultiplier"]
    MAXIMUMCONNECTIONSPERVERTEX_FIELD_NUMBER: _ClassVar[int]
    MAXIMUMCANDIDATESRETURNED_FIELD_NUMBER: _ClassVar[int]
    COMPRESSIONRATIO_FIELD_NUMBER: _ClassVar[int]
    DISTANCEMULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    maximumConnectionsPerVertex: int
    maximumCandidatesReturned: int
    compressionRatio: int
    distanceMultiplier: float
    def __init__(self, maximumConnectionsPerVertex: _Optional[int] = ..., maximumCandidatesReturned: _Optional[int] = ..., compressionRatio: _Optional[int] = ..., distanceMultiplier: _Optional[float] = ...) -> None: ...

class IndexStateResponse(_message.Message):
    __slots__ = ["state"]
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: IndexState
    def __init__(self, state: _Optional[_Union[IndexState, str]] = ...) -> None: ...

class BuildStatusResponse(_message.Message):
    __slots__ = ["indexName", "phases"]
    INDEXNAME_FIELD_NUMBER: _ClassVar[int]
    PHASES_FIELD_NUMBER: _ClassVar[int]
    indexName: str
    phases: _containers.RepeatedCompositeFieldContainer[BuildPhase]
    def __init__(self, indexName: _Optional[str] = ..., phases: _Optional[_Iterable[_Union[BuildPhase, _Mapping]]] = ...) -> None: ...

class BuildPhase(_message.Message):
    __slots__ = ["name", "completionPercentage", "parameters"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    COMPLETIONPERCENTAGE_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    name: str
    completionPercentage: float
    parameters: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[str] = ..., completionPercentage: _Optional[float] = ..., parameters: _Optional[_Iterable[str]] = ...) -> None: ...
