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

from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == "__main__":
    """
        Usage: AWS Connection Simulation
    """
    spark = SparkSession \
        .builder \
        .appName("AWS Connection Simulation") \
        .getOrCreate()

    awsCSVFilePath = sys.argv[1]
    ocideltaTablePath = sys.argv[2]
    awsdeltaTablePath = sys.argv[3]

    original_df = spark.read.format("csv").option("header", "true").load(awsCSVFilePath).withColumn("time_stamp", current_timestamp())
    original_df.write.partitionBy("vendor_id").format("delta").mode("overwrite").save(ocideltaTablePath)
    original_df.write.partitionBy("vendor_id").format("delta").mode("overwrite").save(awsdeltaTablePath)

    print("\nAWS Job Done!!!")
