# VectoriaDB Server Implementation with gRPC Protocol

This project provides the implementation of a server for the VectoriaDB database using the gRPC protocol. 
The distribution is done exclusively through a Docker image.

## Getting Started

To initiate the server, execute the following command:

```bash
../gradlew createDockerContainerDebug
```

This command will:

- Build the Docker image.
- Create a Docker container named `vectoriadb-server-debug`. If a container with this name already exists, it will be replaced.

## Resulting Setup

Upon successful execution, the following setup will be established:

- The server will be accessible on port `9090`.
- Port `5005` will be exposed for remote debugging purposes.
- A directory named `vectoriadb-server` will be created within the build directory, comprising the following sub-directories:
    - `logs`: Contains server log files.
    - `indexes`: Houses index files, with each index stored in a separate directory named after the index.
    - `conf`: Contains configuration files.

All directories will be created with the current user's permissions.

## Running the Container

To run the container, issue the following command:

```bash
docker start vectoriadb-debug-server
```

## Building a Production Version

For a non-debug, production version of the Docker distribution, execute:

```bash
../gradlew clean buildDockerImage
```

This will build an image with the name `vectoriadb/vectoriadb-server:latest`.

### Manual Configuration (Production Version)

Unlike the debug version, in the production version, you will need to manually map port `9090` and bind volumes.
Below is an example command to achieve this:

```bash
docker run -d -p 9090:9090 \
-v /home/user/vectoriadb-server/logs:/vectoriadb-server/logs \
-v /home/user/vectoriadb-server/indexes:/vectoriadb-server/indexes \
-v /home/user/vectoriadb-server/conf:/vectoriadb-server/conf \
--name vectoriadb-server vectoriadb/vectoriadb-server:latest
```
