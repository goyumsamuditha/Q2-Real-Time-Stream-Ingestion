import os
from pyflink.table import TableEnvironment, EnvironmentSettings

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    
    jar_path = f"file://{os.getcwd()}/flink-sql-connector-kafka-1.17.1.jar"
    t_env.get_config().get_configuration().set_string("pipeline.jars", jar_path)
    
    source_ddl = """
    CREATE TABLE traffic_source (
        event_id STRING,
            sensor_id STRING,
            event_timestamp TIMESTAMP(3),
            vehicle_count INT,
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'traffic-telemetry',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-traffic-group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """
    t_env.execute_sql(source_ddl)
    
    sink_ddl = """
    CREATE TABLE windowed_results (
            sensor_id STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_vehicles INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'traffic-window-results',
            'properties.bootstrap.servers' = 'kafka:9092',
            'format' = 'json'
        )
    """
    t_env.execute_sql(sink_ddl)
    
    query = """
    INSERT INTO windowed_results
        SELECT
            sensor_id,
            TUMBLE_START(event_timestamp, INTERVAL '15' MINUTE) AS window_start,
            TUMBLE_END(event_timestamp, INTERVAL '15' MINUTE) AS window_end,
            SUM(vehicle_count) AS total_vehicles
        FROM traffic_source
        GROUP BY
            TUMBLE(event_timestamp, INTERVAL '15' MINUTE),
            sensor_id
    """
    t_env.execute_sql(query)
    print("Flink job for traffic data processing has been submitted.")
    
if __name__ == "__main__":
    main()