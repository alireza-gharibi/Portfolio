CREATE TABLE waste."MyDimDate"(
    date_id INTEGER PRIMARY KEY,
    date date NOT NULL, -- date (no time of day) granularity(resolution) is 1 day.
    day SMALLINT NOT NULL,  --day of month. range: 1-31
    day_of_week SMALLINT NOT NULL, --range: 1-7
    weekday_name CHAR(3) NOT NULL, --monday(Mon) till sunday(Sun)
    week_of_month SMALLINT NOT NULL, --4 weeks of month. range: 1-4
    month SMALLINT NOT NULL, --month number. range: 1-12
    month_name CHAR(3) NOT NULL, -- month name: jan, feb,..., nov, dec
    quarter SMALLINT NOT NULL, -- range: 1-4
    quarter_name CHAR(2) NOT NULL, -- values: Q1,Q2,Q3,Q4
    year SMALLINT NOT NULL  --'integer' types are fatser that fixed 'numeric' types
);

CREATE TABLE waste."MyDimWaste"(
    waste_id SMALLINT PRIMARY KEY,  -- there are limited types of watse: dry, wet, plastic, electronic
    waste_type VARCHAR NOT NULL  -- waste type: dry, wet, plastic, electronic
);

CREATE TABLE waste."MyDimZone"(
    zone_id SMALLINT PRIMARY KEY,
    zone_name VARCHAR NOT NULL,  -- zone name in the city
    city VARCHAR NOT NULL  
);

CREATE TABLE waste."MyFactTrips"(
    trip_id INTEGER PRIMARY KEY,
    waste_id INTEGER NOT NULL, 
    zone_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    waste_collected_in_tons INTEGER NOT NULL  --fact of interest
);

ALTER TABLE waste."MyFactTrips"
    ADD FOREIGN KEY (waste_id) -- making 'waste_id' from 'MyFactTrips' forign key that refrences 'waste_id' from 'MyDimWaste'
    REFERENCES waste."MyDimWaste" (waste_id)
    NOT VALID;

ALTER TABLE waste."MyFactTrips"
    ADD FOREIGN KEY (zone_id) 
    REFERENCES waste."MyDimZone" (zone_id)
    NOT VALID; 

ALTER TABLE waste."MyFactTrips"
    ADD FOREIGN KEY (date_id) 
    REFERENCES waste."MyDimDate" (date_id)
    NOT VALID;
