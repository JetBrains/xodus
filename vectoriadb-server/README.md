Implementation of server for VectoriaDB database with gRPC protocol.
This implementation is distributed exclusively over docker image.

To run server you need to run task: 
```bash
../gradlew clean runServer
```

This task will build docker image and run it. As result:
- Server will be available on port 9090
- Directory vectoriadb-server will be created under the build directory
- Directory vectoriadb-server/logs will contain server log files
- Directory vectoriadb-server/indexes will contain indexes files. 
Each index will be stored in separate directory with name of index.
- Directory vectoriadb-server/conf will contain configuration files.

There is also supplementary task ```stopServer``` which stops server. This task works only with pair 
in ```runServer``` task. So should be used only as finalization task for other build tasks
to start and stop server for functional testing.