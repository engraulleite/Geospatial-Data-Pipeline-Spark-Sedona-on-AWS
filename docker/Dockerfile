FROM public.ecr.aws/emr-serverless/spark/emr-7.9.0:latest

USER root

RUN yum install -y wget

RUN wget \
    https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.7.1/sedona-spark-shaded-3.5_2.12-1.7.1.jar -P /usr/lib/spark/jars/ && \
    wget \
    https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/1.7.1-28.5/geotools-wrapper-1.7.1-28.5.jar -P /usr/lib/spark/jars/  && \
    wget \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar -P /usr/lib/spark/jars/

RUN pip3 install --no-cache-dir \
    geopandas==0.14.3 \
    apache-sedona==1.7.1 \
    pyproj==3.6.1 \
    shapely==2.0.3 \
    fiona \
    rtree \
    requests \
    boto3 \
    botocore \
    typing 


# EMRS executa como hadoop
USER hadoop:hadoop
