#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("ETL")\
    .getOrCreate()

#date = sys.argv[1]
date = "2019-01-01"

url = "jdbc:postgresql://192.168.1.113:32345/airflow?user=airflow&password=airflow"
prop = {"driver": 'org.postgresql.Driver'}

df = spark.read.format('jdbc').options(url=url, dbtable='log', properties=prop).load()

df.write.option("compression", "none").mode('overwrite').parquet("/dags/data/postgres/")

spark.stop()
