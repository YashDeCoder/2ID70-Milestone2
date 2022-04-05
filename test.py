# varString = [('R', 'a', 1), ('S', 'b', 1), ('R', 'b', 1), ('S', 'c', 5)]

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

appName = "PySpark Example - Python Array/List to Spark Data Frame"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# Create a schema for the dataframe
schema = StructType([
    StructField("relation", StringType(), True),
    StructField("attribute", StringType(), True),
    StructField("value", IntegerType(), True)
])

# Convert list to RDD
# rdd = spark.sparkContext.parallelize(varString)
# from pyspark.sql import Row
df = spark.sparkContext.parallelize([ \
    Row(relation='R', attribute='a', value=1), \
    Row(relation='R', attribute='b', value=1), \
    Row(relation='S', attribute='b', value=1), \
    Row(relation='S', attribute='c', value=5)]).toDF()

df2 = df
# Create data frame
# df = spark.createDataFrame(rdd,schema)
# print(df.schema)
# df = df.\
#         where(df['relation'] != df2['relation'] & df['value'] == df2['value'])
df.createTempView("notdf")
# df3 = spark.sql("SELECT DISTINCT first.relation, first.attribute, second.relation, second.attribute FROM notdf as first, notdf as second\
#         WHERE first.relation != second.relation AND first.value == second.value GROUP BY first.relation, first.attribute, second.relation, second.attribute").show()

# df3 = spark.sql("SELECT distinct\
#                 CASE WHEN first.relation < second.relation AND first.attribute < second.attribute THEN first.relation\
#                     ELSE second.relation\
#                     END AS FirstColumn,\
#                 CASE\
#                     WHEN first.relation > second.relation AND first.attribute > second.attribute THEN first.relation\
#                     ELSE second.relation\
#                     END AS SecondColumn\
# FROM notdf as first, notdf as second WHERE first.relation != second.relation AND first.value == second.value GROUP BY first.relation, first.attribute, second.relation, second.attribute").show()


spark.sql("SELECT f.relation, f.attribute, s.relation, s.attribute\
            FROM notdf as f, notdf as s\
            WHERE first.relation != second.relation AND first.value == second.value\
            AND \
            ")