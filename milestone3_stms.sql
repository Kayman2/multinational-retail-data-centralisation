== #Milestone 3

=======================================================================
order_table
=======================================================================
ALTER TABLE orders_table 
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE uuid 
USING user_uuid::uuid;

ALTER TABLE orders_table 
ALTER COLUMN card_number TYPE varchar(19);

ALTER TABLE orders_table 
ALTER COLUMN store_code TYPE varchar(12);

ALTER TABLE orders_table 
ALTER COLUMN product_code TYPE varchar(11);

ALTER TABLE orders_table 
ALTER COLUMN product_quantity TYPE smallint;



=======================================================================
dim_users
=======================================================================

ALTER TABLE dim_users 
ALTER COLUMN first_name TYPE varchar(255);


ALTER TABLE dim_users 
ALTER COLUMN last_name TYPE varchar(255);

ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING NULLIF(date_of_birth, '')::DATE;

ALTER TABLE dim_users 
ALTER COLUMN country_code TYPE varchar(2);

ALTER TABLE dim_users 
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING NULLIF(join_date, '')::DATE;




======================================================================

dim_store_details

======================================================================

-- Change data types in the store_details_table
-- 1. Convert longitude column to FLOAT
ALTER TABLE dim_store_details 
ALTER COLUMN longitude TYPE float using longitude::float;

-- 2. Convert locality column to VARCHAR(255)
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

-- 3. Convert store_code column to VARCHAR(12)
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

-- 4. Convert staff_numbers column to SMALLINT
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;

-- 5. Convert opening_date column to DATE
ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING NULLIF(opening_date, '')::DATE;

-- 6. Convert store_type column to VARCHAR(255) NULLABLE
ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

-- 7. Merge latitude columns (assuming "latitude" and "lat" are the two columns)
-- Set the "latitude" column to the non-null value, and drop "latitude2" if necessary
UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat);

-- 8. Set the "latitude" column to the 0.0 if latitude = 'N/A'
UPDATE dim_store_details
SET latitude = '0.0'
WHERE latitude IS 'N/A';

-- 9. Convert latitude column to FLOAT
ALTER TABLE store_details_table
ALTER COLUMN latitude TYPE FLOAT USING NULLIF(latitude, '')::FLOAT;

-- 10. Convert country_code column to VARCHAR(2)
ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

-- 11. Convert continent column to VARCHAR(255)
ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);

-- 12. Update "location" column to replace null values with 'N/A'
UPDATE dim_store_details
SET locality = 'N/A'
WHERE locality IS NULL;




======================================================================

dim_products

======================================================================

ALTER TABLE dim_products add column weight_class varchar(14)
update dim_products set weight_class =
case 
when weights < 2 then 'Light'
when (weights >= 2  and weights < 40) then 'Mid_Sized'
when (weights >= 40  and weights < 140) then 'Heavy'
when weights >= 140 then 'Truck_Required'
end;

ALTER TABLE dim_products RENAME COLUMN removed TO still_available;
alter table dim_products
alter column still_available
set data type boolean
using case
    when still_available = 'Still_avaliable' then true
    when still_available = 'Removed' then false
    else null
end;

ALTER TABLE dim_products ALTER COLUMN product_price TYPE float using product_price::float;

ALTER TABLE dim_products ALTER COLUMN weight TYPE float using weight::float;

ALTER TABLE dim_products RENAME COLUMN "EAN" TO EAN;
ALTER TABLE dim_products ALTER COLUMN EAN TYPE VARCHAR (17);

ALTER TABLE dim_products ALTER COLUMN product_code TYPE VARCHAR (11);

ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING NULLIF(date_added, '')::DATE;

ALTER TABLE dim_products ALTER COLUMN uuid TYPE uuid 
USING uuid::uuid;
ALTER TABLE dim_products ALTER COLUMN weight_class TYPE VARCHAR (14);
 



======================================================================

dim_date_times  All completed

======================================================================

ALTER TABLE dim_date_times 
ALTER COLUMN month TYPE VARCHAR (2);

ALTER TABLE dim_date_times 
ALTER COLUMN year TYPE VARCHAR (4);

ALTER TABLE dim_date_times 
ALTER COLUMN day TYPE VARCHAR (2);

ALTER TABLE dim_date_times 
ALTER COLUMN time_period TYPE VARCHAR (10);

ALTER TABLE dim_date_times 
ALTER COLUMN date_uuid TYPE uuid 
USING date_uuid::uuid;





======================================================================

dim_card_details

======================================================================
ALTER TABLE dim_card_details 
ALTER COLUMN  expiry_date TYPE VARCHAR (5);

ALTER TABLE dim_card_details 
ALTER COLUMN  card_number TYPE VARCHAR (22);

ALTER TABLE dim_card_details 
ALTER COLUMN date_payment_confirmed TYPE date USING date_payment_confirmed::date;




======================================================================

--- PRIMARY KEY UPDATES

ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);

ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
ALTER TABLE dim_products ADD PRIMARY KEY (product_code);


======================================================================

-- FOREIGN KEY  UPDATES

ALTER TABLE orders_table ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
ALTER TABLE orders_table ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
ALTER TABLE orders_table ADD FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);
ALTER TABLE orders_table ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
ALTER TABLE orders_table ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code);


