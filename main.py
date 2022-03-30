from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2ID70-MS2")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    return SparkContext.getOrCreate(spark_conf)

def wc_flatmap(r):
    print("r", r)
    l = []
    words = r.split(",")
    for word in words:
        l.append(word)
    return l

def wc_mappingrdd(l):
    m = []
    for rec in range(0, len(l)):
        if l[rec] == "R":
            records = l[rec].split(";") 
            numbers = l[rec].split(";")
            for (char, num) in zip(records, numbers):
                m.append(l[rec] + "," + char + "," + num)
        if l[rec] == "S":
            records = l[rec].split(";") 
            numbers = l[rec].split(";")
            for (char, num) in zip(records, numbers):
                m.append(l[rec] + "," + char + "," + num)
        if l[rec] == "T":
            records = l[rec].split(";") 
            numbers = l[rec].split(";")
            for (char, num) in zip(records, numbers):
                m.append(l[rec] + "," + char + "," + num)
    return m

def parse(row):
    records = []
    elements = row.split(',')

    relation_name = elements[0]
    attribute_names = elements[1].split(";")
    attribute_values = elements[2].split(";")

    relation = zip(attribute_names, attribute_values)

    for rel in list(relation):
        record = f"{relation_name}, {rel[0]}, {rel[1]}"
        records.append(record)

    return records

def q1(sc: SparkContext, on_server) -> RDD:
    database_file_path = "/Database.csv" if on_server else "2ID70-2022-MS2-Data-Small\Database.csv"

    # TODO: You may change the value for the minPartitions parameter (template value 160) when running locally.
    # It is advised (but not compulsory) to set the value to 160 when running on the server.
    database_rdd = sc.textFile(database_file_path, 160)
    
    record_groups = database_rdd.map(lambda r: parse(r) if not "AttributeValue" in r else "")
    record_lines = record_groups.flatMap(lambda r: r)
    print(record_lines.take(10))

    
    # TODO: Implement Q1 here by defining q1RDD based on databaseRDD.
    

    q1_rdd = record_lines
    print(">> [q1: R: " + str(q1_rdd.filter(lambda r : r.split(",")[0].__eq__("R")).count()) + "]")
    print(">> [q1: S: " + str(q1_rdd.filter(lambda r : r.split(",")[0].__eq__("S")).count()) + "]")
    print(">> [q1: T: " + str(q1_rdd.filter(lambda r : r.split(",")[0].__eq__("T")).count()) + "]")

    return q1_rdd


def q2(spark_context: SparkContext, q1_rdd: RDD):
    spark_session = SparkSession(spark_context)
    pass

    # TODO: Implement Q2 here.


def q3(spark_context: SparkContext, q1_rdd: RDD):
    spark_session = SparkSession(spark_context)
    pass
    # TODO: Implement Q3 here.


def q4(spark_context: SparkContext, on_server):

    pass
    # streaming_context = StreamingContext(spark_context, 2)
    # streaming_context.checkpoint("checkpoint")

    # hostname = "stream-host" if on_server else "localhost"
    # lines = streaming_context.socketTextStream(hostname, 9000)

    # # TODO: Implement Q4 here.

    # # Start the streaming context, run it for two minutes or until termination
    # streaming_context.start()
    # streaming_context.awaitTerminationOrTimeout(2 * 60)
    # streaming_context.stop()


# Main 'function' which initializes a Spark context and runs the code for each question.
# To skip executing a question while developing a solution, simply comment out the corresponding function call.
if __name__ == '__main__':

    on_server = False  # TODO: Set this to true if and only if running on the server

    spark_context = get_spark_context(on_server)

    spark_context.setLogLevel("OFF")

    q1_rdd = q1(spark_context, on_server)

    q2(spark_context, q1_rdd)

    q3(spark_context, q1_rdd)

    q4(spark_context, on_server)

    spark_context.stop()
