FROM apache/airflow:2.5.0

USER root

# Install OpenJDK-11, ant, and wget
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ant wget && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Create directory for Spark jars
RUN mkdir -p /opt/spark/jars

# Download Hadoop AWS and AWS Java SDK jars
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.563/aws-java-sdk-bundle-1.11.563.jar -P /opt/spark/jars/

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
