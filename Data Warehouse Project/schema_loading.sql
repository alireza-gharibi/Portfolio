--Creating data warehouse Schema: 

CREATE TABLE "dim_date" 
(
    date_id INTEGER PRIMARY KEY,
    date date NOT NULL, -- date (no time of day) granularity(resolution) is 1 day.
    year SMALLINT NOT NULL,  --'integer' types are fatser that fixed 'numeric' types
    quarter SMALLINT NOT NULL, -- range: 1-4
    quarter_name CHAR(2) NOT NULL, -- values: Q1,Q2,Q3,Q4
    month SMALLINT NOT NULL, --month number. range: 1-12
    month_name VARCHAR(10) NOT NULL, -- month name: january, february,..., november, december
    day SMALLINT NOT NULL,  --day of month. range: 1-31
    day_of_week SMALLINT NOT NULL, --range: 1-7
    weekday_name VARCHAR(10) NOT NULL --monday till sunday
);

CREATE TABLE "dim_truck"(
    truck_id SMALLINT PRIMARY KEY, 
    truck_type VARCHAR(10) NOT NULL  
);

CREATE TABLE "dim_station"(
    station_id SMALLINT PRIMARY KEY, 
    city VARCHAR(20) NOT NULL  
);

CREATE TABLE "fact_trips"(
    trip_id INTEGER PRIMARY KEY,
    date_id INTEGER NOT NULL,
    station_id SMALLINT REFERENCES "dim_station" (station_id),  -- adding foreign key
    truck_id SMALLINT REFERENCES "dim_truck" (truck_id ),  -- adding foreign key
    Waste_collected NUMERIC(10,2) NOT NULL   --fact of interest, 2 digit decimal number and all digits to both sides of the decimal are 10
);



-- populating tables:

\COPY dim_truck FROM DimTruck.csv WITH CSV HEADER;
\COPY dim_date FROM DimDate.csv WITH CSV HEADER;
\COPY dim_station FROM DimStation.csv WITH CSV HEADER;
\COPY fact_trips FROM FactTrips.csv WITH CSV HEADER;
