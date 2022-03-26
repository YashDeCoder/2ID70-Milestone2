from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2ID70-MS2")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    return SparkContext.getOrCreate(spark_conf)

def wc_flatmap(r):
    l = []
    words = r.split(",")
    for word in words:
        l.append(word)
    return l

def wc_mappingrdd(l):
    m = []
    for rec in range(0, len(l)):
        if l[rec].equals("R"):
            records = l[rec].split(";") 
            numbers = l[rec].split(";")
            for (char, num) in zip(records, numbers):
                m.append(l[rec] + "," + char + "," + num)
        if l[rec].equals("S"):
            records = l[rec].split(";") 
            numbers = l[rec].split(";")
            for (char, num) in zip(records, numbers):
                m.append(l[rec] + "," + char + "," + num)
        if l[rec].equals("T"):
            records = l[rec].split(";") 
            numbers = l[rec].split(";")
            for (char, num) in zip(records, numbers):
                m.append(l[rec] + "," + char + "," + num)
    return m

def q1(spark_context: SparkContext, on_server) -> RDD:
    database_file_path = "/Database.csv" if on_server else "2ID70-2022-MS2-Data-Small\Database.csv"

    # TODO: You may change the value for the minPartitions parameter (template value 160) when running locally.
    # It is advised (but not compulsory) to set the value to 160 when running on the server.
    database_rdd = spark_context.textFile(database_file_path, 160)

    # record_per_word_rdd = database_rdd\
    #     .flatMap(lambda r: wc_flatmap(r))

    # one_per_word = record_per_word_rdd\
    #     .map(lambda r: (r, 1))

    # count_per_word = one_per_word\
    #     .reduceByKey(lambda a, b: a + b)

    # count_per_word_list = count_per_word.collect()
    record_per_word_rdd = database_rdd\
        .flatMap(lambda r: wc_flatmap(r))
    split_further_word = record_per_word_rdd\
        .flatMap(lambda r: wc_mappingrdd(r))
    
    # TODO: Implement Q1 here by defining q1RDD based on databaseRDD.
    q1_rdd = (split_further_word)
    print(">> [q1: R: " + str(q1_rdd.filter(lambda r : r.split(",")[0].equals("R").count())).split(":")[1] + "]")
    print(">> [q1: S: " + q1_rdd.filter(lambda r : r.split(",")[0].equals("S").count()).split(":")[1] + "]")
    print(">> [q1: T: " + q1_rdd.filter(lambda r : r.split(",")[0].equals("T").count()).split(":")[1] + "]")

    return q1_rdd


def q2(spark_context: SparkContext, q1_rdd: RDD):
    spark_session = SparkSession(spark_context)

    # TODO: Implement Q2 here.


def q3(spark_context: SparkContext, q1_rdd: RDD):
    spark_session = SparkSession(spark_context)
    # TODO: Implement Q3 here.


def q4(spark_context: SparkContext, on_server):
    streaming_context = StreamingContext(spark_context, 2)
    streaming_context.checkpoint("checkpoint")

    hostname = "stream-host" if on_server else "localhost"
    lines = streaming_context.socketTextStream(hostname, 9000)

    # TODO: Implement Q4 here.

    # Start the streaming context, run it for two minutes or until termination
    streaming_context.start()
    streaming_context.awaitTerminationOrTimeout(2 * 60)
    streaming_context.stop()


# Main 'function' which initializes a Spark context and runs the code for each question.
# To skip executing a question while developing a solution, simply comment out the corresponding function call.
if __name__ == '__main__':

    on_server = False  # TODO: Set this to true if and only if running on the server

    spark_context = get_spark_context(on_server)

    q1_rdd = q1(spark_context, on_server)

    q2(spark_context, q1_rdd)

    q3(spark_context, q1_rdd)

    q4(spark_context, on_server)

    spark_context.stop()
