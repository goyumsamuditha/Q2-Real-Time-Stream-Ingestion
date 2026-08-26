import os
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.common import Configuration

os.environ['_JAVA_OPTIONS'] = "--add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"


def main():
    config = Configuration()
    config.set_integer("rest.port", 8082)
    
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().with_configuration(config).build()
    t_env = TableEnvironment.create(env_settings)
    
    jar_abspath = os.path.abspath("flink-sql-connector-kafka-1.17.1.jar")
    jar_abspath_forward = jar_abspath.replace('\\', '/').replace(' ', '%20')    
    jar_uri = f"file:///{jar_abspath_forward}"
    
    print(f"Deploying JAR file to Flink cluster: {jar_uri}")
    print(f"Flink Dashboard live at : http://localhost:8082")
    t_env.get_config().get_configuration().set_string("pipeline.jars", jar_uri)
    
    source_ddl = """
    CREATE TABLE traffic_source (
        event_id STRING,
        sensor_id STRING,
        count_date TIMESTAMP(3),
        turn_count INT,
        WATERMARK FOR count_date AS count_date - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'traffic-telemetry',
        'properties.bootstrap.servers' = 'localhost:9092', 
        'properties.group.id' = 'flink-traffic-group-assignment',
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
        'properties.bootstrap.servers' = 'localhost:9092',
        'format' = 'json'
    )
    """
    t_env.execute_sql(sink_ddl)
    
    query = """
    INSERT INTO windowed_results
        SELECT
            sensor_id,
            TUMBLE_START(count_date, INTERVAL '10' MINUTE) AS window_start,
            TUMBLE_END(count_date, INTERVAL '10' MINUTE) AS window_end,
            SUM(turn_count) AS total_vehicles
        FROM traffic_source
        GROUP BY
            TUMBLE(count_date, INTERVAL '10' MINUTE),
            sensor_id
    """
    

    print("Starting Flink debug job. Watch this terminal for output rows...")

    t_env.execute_sql(query).wait()

if __name__ == "__main__":
    main()