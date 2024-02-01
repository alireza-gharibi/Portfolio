## Project Scenario
I am a data engineer hired by a solid waste management company. The company collects and recycles solid waste across major cities in the country of Brazil. The company operates hundreds of trucks of different types to collect and transport solid waste. The company would like to create a data warehouse so that it can create reports like:
- total waste collected per year per city
- total waste collected per month per city
- total waste collected per quarter per city
- total waste collected per year per trucktype
- total waste collected per trucktype per city
- total waste collected per trucktype per station per city 

The solid waste management company has provied me the sample data they wish to collect:

![Alt text](https://github.com/alireza-gharibi/Portfolio/blob/main/Data%20Warehouse%20Project/solid-waste-trips-new.png)
I will use my data warehousing skills to design and implement a data warehouse for the company. \
The dataset i would be using in this project is not a real life dataset. It was programmatically created for this project purpose.

## Data Modeling
I will start my project by designing a Star Schema warehouse by identifying the columns for the various dimension and fact tables in the schema.
### Designing the dimension table **MyDimDate**:
The company is looking at a granularity of day. Which means they would like to have the ability to generate the report on yearly, monthly, daily, and weekday basis. Here is a list of fields:\
**date_id, date, day, day_of_week, weekday_name, week_of_month,  month, month_name, quarter, quarter_name , year**
### Designing the dimension table **MyDimWaste**:
**waste_id, waste_type**
### Designing the dimension table **MyDimZone**:
**zone_id, zone_name, city**
### Designing the fact table **MyFactTrips**:
**trip_id, waste_id, zone_id, date_id, waste_collected_in_tons**
## Change of plans
After the initial schema design [first schema.sql](https://github.com/alireza-gharibi/Portfolio/blob/main/Data%20Warehouse%20Project/First%20Schema.sql), i was told that due to opertional issues, data could not be collected in the format initially planned.This implies that the previous tables **(MyDimDate, MyDimWaste, MyDimZone, MyFactTrips)** and their associated attributes are no longer applicable to the current design. The company has loaded data using CSV files per the new design. I will load the data provided by the company in csv format.
## New Schema
I redesign the schema.
Now i load the database using csv files provided by the company.\
I create the [schema](https://github.com/alireza-gharibi/Portfolio/blob/main/Data%20Warehouse%20Project/schema_loading.sql) and populate the tables using following command:
```
psql -h localhost -U postgres -d postgres -f schema_loading.sql
```
 we can also use **pgadmin** to load csv files.(i prefer **psql**)
### Query 1
Create a **grouping sets** query using the columns station_id, truck_type, total waste_collected: 
```
select station_id, truck_type, sum(waste_collected) as "total waste collected"
from fact_trips f 
inner join dim_truck d
on f.truck_id = d.truck_id
group by grouping sets(station_id, truck_type);
```
### Query 2
Create a **rollup** query using the columns year, city, stationid, and total waste collected:
```
select year, city, s.station_id, sum(waste_collected) as "total waste collected"
from fact_trips f 
inner join dim_date d
on f.date_id = d.date_id
inner join dim_station s
on f.station_id = s.station_id 
group by rollup(year, city, s.station_id);
```
### Query 3
Create a **cube** query using the columns year, city, stationid, and average waste collected:
```
select year, city, f.station_id, avg(waste_collected) as "average waste collected"
from fact_trips f 
inner join dim_date d
on f.date_id = d.date_id
inner join dim_station s
on f.station_id = s.station_id 
group by cube (year, city, f.station_id);

```
### Query 4
Create a **materialzed view** named max_waste_stats using the columns city, stationid, trucktype, and max waste collected:
```
create materialized view max_waste_stats as
select city, f.station_id, truck_type, max(waste_collected) as "max waste collected"
from fact_trips f 

inner join dim_truck t
on f.truck_id = t.truck_id 

inner join dim_station s
on f.station_id = s.station_id 

group by cube (truck_type, city, f.station_id);
```









