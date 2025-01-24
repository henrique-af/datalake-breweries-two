FROM apache/airflow:2.7.3

USER root

# Install OpenJDK 11 for Spark components
RUN apt-get update && apt-get install -y openjdk-11-jdk ant wget && apt-get clean

# Set the JAVA_HOME environment variable for Java 11
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Create a directory to store additional JAR files
RUN mkdir -p /opt/spark/jars

# Download Hadoop AWS connector and AWS SDK for S3-compatible storage
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/3.3.2/spark-hadoop-cloud_2.12-3.3.2.jar -P /opt/spark/jars/

# Download the PostgreSQL JDBC driver (used by Spark to write/read data in Postgres)
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -P /opt/spark/jars/

USER airflow

# Copy in the Python requirements file
COPY requirements.txt /requirements.txt

# Upgrade pip and install Python dependencies defined in requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
