from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from argparse import ArgumentParser
import os

import tempfile
import shutil

path = r"\\TERSANWEB\temp\client-logs"


def create_temporary_copy(path, file_name):
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, file_name)
    shutil.copy2(path, temp_path)
    return temp_path


files = []
temp_files = []

# create list of file path
for file in os.listdir(path):
    if file.endswith(".txt"):
        files.append([os.path.join(path, file), file])

# create temp files
for i in files:
    temp = create_temporary_copy(i[0], i[1])
    temp_files.append(temp)

parser = ArgumentParser()
parser.add_argument("-p", "--path",
                    default=r"\\TERSANWEB\temp\client-logs",
                    help="path of files", metavar="PATH")
parser.add_argument("-r", "--rows",
                    default=20,
                    help="Number of rows to show")
parser.add_argument("-s", "--showall",
                    default=True,
                    help="Show full value of cell")
parser.add_argument("-q", "--query",
                    default="select * from log",
                    help="SQL Query")

args = parser.parse_args()

spark = SparkSession.builder.appName("DataFrame Log").getOrCreate()

schema = StructType([
    StructField("TARIH", StringType(), True),
    StructField("ISIM", StringType(), True),
    StructField("TIP", StringType(), True),
    StructField("IP", StringType(), True),
    StructField("URL", StringType(), True)])

df = spark.read.option("delimiter", "|").\
    option("header", "false").\
    schema(schema).\
    option("inferSchema", "true").\
    csv(temp_files)

df.createOrReplaceTempView("log")
df2 = spark.sql(args.query)

df2.show(int(args.rows), args.showall)

# delete temp files
for x in temp_files:
    os.remove(x)
