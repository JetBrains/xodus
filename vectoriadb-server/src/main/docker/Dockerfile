FROM eclipse-temurin:21-alpine
EXPOSE 9090
RUN mkdir /vectoriadb
COPY build/libs/*.jar /vectoriadb/bin/
VOLUME ["/vectoriadb/logs", "/vectoriadb/config", "/vectoriadb/indexes"]
WORKDIR /vectoriadb
ENTRYPOINT ["java", "-Dvectoriadb.server.base-path=/vectoriadb" ,"-cp", "/vectoriadb/bin/*", \
"--enable-preview", "-Dvectoriadb.log.dir=/vectoriadb/logs", "jetbrains.vectoriadb.server.ApplicationInitializer"]