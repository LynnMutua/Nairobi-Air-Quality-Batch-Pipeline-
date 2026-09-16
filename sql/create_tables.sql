CREATE TABLE IF NOT EXISTS raw_air_quality (
    sensor_id BIGINT NOT NULL,
    measurement_timestamp TIMESTAMPTZ NOT NULL,
    location_id BIGINT NOT NULL,
    location_name VARCHAR,
    parameter TEXT NOT NULL,
    unit TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    CONSTRAINT pk_raw_air_quality PRIMARY KEY (sensor_id, measurement_timestamp)
);

