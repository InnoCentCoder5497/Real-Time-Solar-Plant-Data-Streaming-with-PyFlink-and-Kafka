CREATE SCHEMA solar
    AUTHORIZATION admin;

CREATE TABLE IF NOT EXISTS solar.daily_yield
(
    "plant_id" integer,
    "source_key" character varying,
    "date_time" timestamp without time zone,
    "yield" double precision,
    rolling_sum double precision,
    PRIMARY KEY ("plant_id", "source_key", "date_time")
);

CREATE TABLE IF NOT EXISTS solar.generator_metrics
(
    plant_id integer,
    source_key character varying,
    date_time timestamp without time zone,
    dc_power double precision,
    ac_power double precision,
    yield double precision,
    PRIMARY KEY ("plant_id", "source_key", "date_time")
);

CREATE TABLE IF NOT EXISTS solar.weather_metrics
(
    plant_id integer,
    source_key character varying,
    date_time timestamp without time zone,
    ambient_temp double precision,
    module_temp double precision,
    irradiation double precision,
    PRIMARY KEY ("plant_id", "source_key", "date_time")
);

TRUNCATE TABLE solar.daily_yield;
TRUNCATE TABLE solar.weather_metrics;
TRUNCATE TABLE solar.generator_metrics;

ALTER TABLE IF EXISTS solar.daily_yield
    OWNER to admin;

ALTER TABLE IF EXISTS solar.generator_metrics
    OWNER to admin;

ALTER TABLE IF EXISTS solar.weather_metrics
    OWNER to admin;

SELECT create_hypertable('solar.daily_yield', 'date_time');
SELECT create_hypertable('solar.weather_metrics', 'date_time');
SELECT create_hypertable('solar.generator_metrics', 'date_time');