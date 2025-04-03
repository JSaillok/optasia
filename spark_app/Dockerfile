FROM bitnami/spark:3.3.1

# Install SQLite JDBC driver
RUN curl -L -o /opt/spark/jars/sqlite-jdbc-3.36.0.3.jar \
    https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.3/sqlite-jdbc-3.36.0.3.jar

# Copy scripts and data
COPY scripts /scripts
COPY data /data

WORKDIR /scripts