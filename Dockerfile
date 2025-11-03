FROM adityakr123/pyspark-env

WORKDIR /app

# Copy your PySpark app and any dependencies (like ojdbc8.jar & jsch.jar)
COPY . .

# Add JSch JAR to Spark's classpath
# (Spark loads all jars from /opt/spark/jars automatically)
RUN mkdir -p /opt/spark/jars && \
    cp jsch-0.1.55.jar /opt/spark/jars/ && \
    cp ojdbc8.jar /opt/spark/jars/

# Optional: You can specify your default script here
# CMD [ "spark-submit", "--jars", "/opt/spark/jars/jsch-0.1.55.jar,/opt/spark/jars/ojdbc8.jar", "temp.py" ]
