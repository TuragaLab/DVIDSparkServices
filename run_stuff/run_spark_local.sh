export PATH=$PATH:/groups/turaga/home/grisaitisw/opt/spark-2.1.0-bin-hadoop2.7/bin
export PYSPARK_PYTHON=/groups/turaga/home/grisaitisw/anaconda2/envs/dvidspark-46faeeb/bin/python

spark-submit \
    --master local[75] \
    --driver-memory 50g \
    --conf spark.driver.maxResultSize=50g \
    launch_stitches.py
