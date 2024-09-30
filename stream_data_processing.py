import os
import math
import time
from kafka import KafkaAdminClient
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.table.udf import udf, ScalarFunction
from pyflink.common import WatermarkStrategy
from pyflink.table import (DataTypes, Schema, StreamTableEnvironment)


class HASHtoINT(ScalarFunction):
    def __init__(self, hash_bucket_size):
        self.hash_bucket_size = hash_bucket_size

    def eval(self, data):
        return abs(int(data[:7], 16) % self.hash_bucket_size)


class UserIdTransformer(ScalarFunction):
    def eval(self, profile_id, user_name):
        if profile_id == -1 or math.isnan(int(profile_id)):
            return user_name
        else:
            return profile_id


class IsLastWatch(ScalarFunction):
    def eval(self, source, duration):
        if source in ['movie_watch_history', 'movie_watch_history_ab'] and duration >= 10:
            return True
        elif source in ['vod_watch_history', 'vod_watch_history_ab'] and duration >= 40:
            return True
        else:
            return False


if __name__ == '__main__':
    list_source = ['movie_watch_history', 'movie_watch_history_ab',
                   'vod_watch_history', 'vod_watch_history_ab']
    print('Wait for connecting to Kafka ........................')
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
            kafka_topics = admin._client.cluster.topics()
            check = all(item in kafka_topics for item in list_source)
            if check:
                print("============== Success connect Kafka %s ==============" % 'localhost:9092')
                break
        except KafkaTimeoutError as kte:
            print("KafkaLogsProducer Error! ", kte)
            time.sleep(3)
        except NoBrokersAvailable as nbe:
            print("NoBrokersAvailable Error! ", nbe)
            time.sleep(3)
        except ValueError as vle:
            print("ValueError Error! ", vle)
            time.sleep(3)

    if check:
        print('Start processing')
        env = StreamExecutionEnvironment.get_execution_environment()
        kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                 'flink-sql-connector-kafka-1.17.0.jar')
        env.add_jars("file://{}".format(kafka_jar))
        env.set_parallelism(1)
        t_env = StreamTableEnvironment.create(env)

        row_type_info = Types.ROW_NAMED(
            ['source', 'date_time', 'content_id', 'content_type',
             'profile_id', 'username', 'duration'],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
             Types.STRING(), Types.STRING(), Types.INT()])
        json_format = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()

        source = KafkaSource.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_topics('movie_watch_history', 'movie_watch_history_ab',
                        'vod_watch_history', 'vod_watch_history_ab') \
            .set_group_id("group-1") \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(json_format) \
            .build()
        ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

        table_source = t_env.from_data_stream(ds, Schema.new_builder()
                                              .column("source", DataTypes.STRING())
                                              .column_by_expression("ts",
                                                                    "TO_TIMESTAMP(date_time)")
                                              .column("date_time", DataTypes.STRING())
                                              .column("content_id", DataTypes.STRING())
                                              .column("content_type", DataTypes.STRING())
                                              .column("profile_id", DataTypes.STRING())
                                              .column("username", DataTypes.STRING())
                                              .column("duration", DataTypes.INT())
                                              .build())
        t_env.create_temporary_view("tblRaw", table_source)

        t_env.create_temporary_function("hash_to_int", udf(HASHtoINT(hash_bucket_size=100),
                                                           input_types=[DataTypes.STRING()],
                                                           result_type=DataTypes.INT()))

        t_env.create_temporary_function("user_id_transformer",
                                        udf(UserIdTransformer(),
                                            input_types=[DataTypes.STRING(),
                                                         DataTypes.STRING()],
                                            result_type=DataTypes.STRING()))

        t_env.create_temporary_function("is_last_watch",
                                        udf(IsLastWatch(), input_types=[DataTypes.STRING(),
                                                                        DataTypes.INT()],
                                            result_type=DataTypes.BOOLEAN()))

        t_env.execute_sql("""
            CREATE TABLE online_feature (
                source String,
                date_time String,
                user_id String,
                last_watched_hashed_item_id_v1 BIGINT,
                last_watched_hashed_item_id_v2 BIGINT
                ) with (
                'connector' = 'kafka',
                'topic' = 'online_feature',
                'properties.bootstrap.servers' = 'localhost:9092',
                'properties.group.id' = 'group',
                'scan.startup.mode' = 'earliest-offset',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true',
                'format' = 'json'
            )
            """)
        tblOnlineFeature = t_env.sql_query("select source, date_time, "
                                           "user_id_transformer(profile_id, "
                                           "username) AS user_id, "
                                           "hash_to_int(MD5(content_type|| '#' ||content_id)) "
                                           "AS hashed_content_feature_transformer_v1, "
                                           "hash_to_int(SHA1(content_type|| '#' ||content_id)) "
                                           "AS hashed_content_feature_transformer_v2 "
                                           "FROM tblRaw "
                                           "where is_last_watch(source, duration) = True;")
        tblOnlineFeature.execute()

        t_env.create_temporary_view("tblOnlineFeature", tblOnlineFeature)

        stmt_set = t_env.create_statement_set()
        stmt_set \
            .add_insert_sql("INSERT INTO online_feature SELECT * FROM tblOnlineFeature;")
        stmt_set.execute()
        t_env.sql_query('select * from online_feature;').execute().print()
