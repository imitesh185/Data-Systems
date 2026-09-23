FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN mkdir -p /opt/spark/ivy /opt/deltasync/jobs /data/delta /data/checkpoints \
    && chown -R 185:185 /opt/spark/ivy /opt/deltasync /data

COPY --chown=185:185 spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY --chown=185:185 jobs/cdc_to_delta.py /opt/deltasync/jobs/cdc_to_delta.py

USER 185

# Resolve and cache the pinned Delta and Kafka connector jars in the image.
RUN /opt/spark/bin/spark-submit \
      --master "local[1]" \
      --conf spark.ui.enabled=false \
      --class org.apache.spark.examples.SparkPi \
      local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar 1 \
      >/dev/null

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["/opt/deltasync/jobs/cdc_to_delta.py"]
