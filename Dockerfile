FROM bitnami/spark:3.5.0

# Switch to root user to install packages
USER root

# Install system dependencies for confluent-kafka
RUN apt-get update && \
    apt-get install -y \
        python3-pip \
        librdkafka-dev \
        gcc \
        g++ \
        && \
    pip3 install --no-cache-dir \
        requests \
        kafka-python \
        confluent-kafka \
        simplejson \
        boto3 \
        pandas \
        pytz \
        awscli \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to spark user
USER 1001

# Set working directory
WORKDIR /opt/bitnami/spark

