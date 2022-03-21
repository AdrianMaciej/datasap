FROM docker.io/bitnami/spark:3

COPY conf/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
