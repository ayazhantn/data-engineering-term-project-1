-- Create the database
DROP DATABASE IF EXISTS google_merchandise_db;
CREATE DATABASE google_merchandise_db;
USE google_merchandise_db;

-- Create the events table
DROP TABLE IF EXISTS events;
CREATE TABLE events (
    user_id VARCHAR(255),
    ga_session_id VARCHAR(255),
    country VARCHAR(100),
    device VARCHAR(100),
    type VARCHAR(100),
    item_id VARCHAR(255),
    date DATETIME
);

-- Create the items table
DROP TABLE IF EXISTS items;
CREATE TABLE items (
    item_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    brand VARCHAR(100),
    variant VARCHAR(100),
    category VARCHAR(100),
    price_in_usd DECIMAL(10, 2)
);

-- Create the users table
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    user_id VARCHAR(255) PRIMARY KEY,
    ltv DECIMAL(10, 2),
    date DATETIME
);

-- ---------------------------------------------------
-- ETL PIPELINE: first step
-- EXTRACT data from CSV into staging tables
-- ---------------------------------------------------

-- Check configuration settings
SHOW VARIABLES LIKE 'secure_file_priv'; -- should not be "NULL"
SHOW VARIABLES LIKE 'local_infile';     -- should not "ON"

-- For deleting secure_file_priv from NULL I have a New Configuration File  
-- First, it is needed to Check for the Existence of my.cnf & change the configuration setting
-- Since there was no my.cnf on my computer, I created it in Terminal
-- a line to run on Terminal: sudo nano /etc/my.cnf
-- I added basic configuration: [mysqld] secure-file-priv="", saved it and restart MySQLWorkbench
-- As a result, it made the value of 'secure_file_priv' empty, getting rid of "NULL". 

-- For setting local_infile value ON
SET GLOBAL local_infile = 1;

-- Once the configuration is set up, csv files should be in the required directory
-- Check the directory
SELECT @@datadir;            -- result: "/usr/local/mysql/data/"

-- 3 csv files were moved to the directory
-- Importing data from csv files is now possible
-- Since I am using Load Data Infile command, I could not apply the stored procedure here

-- Alternatively, it is possible to use "Table Data Import Wizard"

-- Import data into events table
LOAD DATA INFILE '/usr/local/mysql/data/events1.csv'
INTO TABLE events
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(user_id, ga_session_id, country, device, type, item_id, date);

-- Import data into items table
LOAD DATA INFILE '/usr/local/mysql/data/items.csv'
INTO TABLE items
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(item_id, name, brand, variant, category, price_in_usd);

-- Import data into users table
LOAD DATA INFILE '/usr/local/mysql/data/users.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(user_id, ltv, date);

-- ---------------------------------------------------
-- ETL PIPELINE: Transformation
-- CREATE FOREIGN KEY RELATIONSHIPS
-- ---------------------------------------------------

-- Add a foreign key constraint on item_id in Events
-- Create a relationship with the item_id column in Items.
ALTER TABLE Events
ADD CONSTRAINT fk_event_item
FOREIGN KEY (item_id)
REFERENCES Items(item_id);
-- Add a foreign key constraint on user_id in Events
-- Create a relationship with the user_id column in Users.
ALTER TABLE Events
ADD CONSTRAINT fk_event_user
FOREIGN KEY (user_id)
REFERENCES Users(user_id);

-- ---------------------------------------------------
-- ETL PIPELINE: Transformation
-- Check for missing values, duplicates,
-- Ensure primary key existence.
-- ---------------------------------------------------

-- MISSING VALUES:

-- Create a procedure to count missing values in each column of each table:
-- Create a table to store missing values number for each column
DROP TABLE IF EXISTS MissingValuesSummary;
CREATE TABLE MissingValuesSummary (
    table_name VARCHAR(255),
    column_name VARCHAR(255),
    missing_count INT
);
-- The procedure uses a dynamic SQL query
DELIMITER //
DROP PROCEDURE IF EXISTS CheckMissingValuesForColumn;
CREATE PROCEDURE CheckMissingValuesForColumn(IN tbl_name VARCHAR(255), IN col_name VARCHAR(255))
BEGIN
	-- This is a dynamic SQL query to count missing (NULL or empty)
    SET @query = CONCAT(
        'INSERT INTO MissingValuesSummary (table_name, column_name, missing_count) ',
        'SELECT "', tbl_name, '", "', col_name, '", COUNT(*) ',
        'FROM ', tbl_name, 
        ' WHERE ', col_name, ' IS NULL OR TRIM(', col_name, ') = ""'
    );
    
    -- Prepare the dynamic query for execution
    PREPARE stmt FROM @query;
    -- Execute the prepared dynamic query
    EXECUTE stmt;
	-- Deallocate the dynamic query
    DEALLOCATE PREPARE stmt;
END 
DELIMITER ;

-- Loop through 'items' table columns
CALL CheckMissingValuesForColumn('items', 'item_id');
CALL CheckMissingValuesForColumn('items', 'name');
CALL CheckMissingValuesForColumn('items', 'brand');
CALL CheckMissingValuesForColumn('items', 'variant');
CALL CheckMissingValuesForColumn('items', 'category');
CALL CheckMissingValuesForColumn('items', 'price_in_usd');

-- Loop through 'events' table columns
CALL CheckMissingValuesForColumn('events', 'user_id');
CALL CheckMissingValuesForColumn('events', 'ga_session_id');
CALL CheckMissingValuesForColumn('events', 'country');
CALL CheckMissingValuesForColumn('events', 'device');
CALL CheckMissingValuesForColumn('events', 'type');
CALL CheckMissingValuesForColumn('events', 'item_id');
CALL CheckMissingValuesForColumn('events', 'date');

-- Loop through 'users' table columns
CALL CheckMissingValuesForColumn('users', 'user_id');
CALL CheckMissingValuesForColumn('users', 'ltv');
CALL CheckMissingValuesForColumn('users', 'date');

-- Check the results: which column has how many missing values
SELECT table_name, column_name, missing_count
FROM MissingValuesSummary
WHERE missing_count > 0;
-- Result: 'variant' column has 408 missing values, other columns do not have missing values.
-- This is ok since not all items have variants.
-- 'country' column in 'events' table has 4555 missing values. This might be due to privacy settings of not sharing the location.

-- I still want to keep a table of events with missing values:
-- Create a new table & store the original events with missing values in the 'country' column
CREATE TABLE events_with_missing_country AS
SELECT *
FROM events
WHERE country IS NULL OR TRIM(country) = '';

-- Remove rows with missing 'country' from the original 'events' table
DELETE FROM events
WHERE country IS NULL OR TRIM(country) = '';
SELECT COUNT(*) FROM events;

-- DUPLICATES:
-- Check for duplicates in items table
SELECT item_id, name, brand, variant, category, price_in_usd, COUNT(*) AS duplicate_count
FROM items
GROUP BY item_id, name, brand, variant, category, price_in_usd
HAVING COUNT(*) > 1;
-- Result: No duplicate rows

-- Check for duplicates in users table
SELECT user_id, ltv, date, COUNT(*) AS duplicate_count
FROM users
GROUP BY user_id, ltv, date
HAVING COUNT(*) > 1;
-- Result: No duplicate rows

-- Check for duplicated id's of items & users in the tables
SELECT item_id, COUNT(*) AS duplicate_count
FROM items
GROUP BY item_id
HAVING COUNT(*) > 1;

SELECT user_id, COUNT(*) AS duplicate_count
FROM users
GROUP BY user_id
HAVING COUNT(*) > 1;
-- Result: all the id's are unique

-- 'events' table transformation:
-- 'events' table does not have a primary key originally:
-- Add Primary Key to the 'events' table
ALTER TABLE events ADD COLUMN event_id INT AUTO_INCREMENT PRIMARY KEY;

-- Check for duplicates in events table
DELIMITER //
DROP PROCEDURE IF EXISTS CheckDuplicatesInEvents;
CREATE PROCEDURE CheckDuplicatesInEvents()
BEGIN
    -- Temporary table to store the duplicate rows
    CREATE TEMPORARY TABLE duplicate_events AS
    SELECT 
        user_id, ga_session_id, country, device, type, item_id, date, 
        COUNT(*) AS duplicate_count
    FROM events
    GROUP BY user_id, ga_session_id, country, device, type, item_id, date
    HAVING COUNT(*) > 1;

    -- Display the duplicates with their counts
    SELECT * FROM duplicate_events;

    -- Clean up by dropping the temporary table
    DROP TEMPORARY TABLE IF EXISTS duplicate_events;
END //
DELIMITER ;

CALL CheckDuplicatesInEvents();
-- Result: There are duplicated rows.
-- Since the 'date' column has hour:minute:second marker, 
-- any duplicates are not expected.
-- But this duplicates might have occured because of internet problems of the user.
-- Consider deleting the duplicates:

-- Disable safe update mode (to be able to drop some rows)
SET SQL_SAFE_UPDATES = 0;

-- Procedure to delete duplicates
DELIMITER $$
DROP PROCEDURE IF EXISTS CleanDuplicateEvents;
CREATE PROCEDURE CleanDuplicateEvents()
BEGIN
    -- Create a temporary table to store the event_ids to keep (the first event per group)
    CREATE TEMPORARY TABLE temp_events_to_keep AS
    SELECT MIN(event_id) AS event_id
    FROM events
    GROUP BY ga_session_id, user_id, item_id, type, device, date;
    -- Delete the duplicate events that are not in the temp table
    DELETE FROM events
    WHERE event_id NOT IN (SELECT event_id FROM temp_events_to_keep);
    -- Drop the temporary table after cleaning
    DROP TEMPORARY TABLE IF EXISTS temp_events_to_keep;
END$$
DELIMITER ;

-- Delete the duplicates
CALL CleanDuplicateEvents();

-- Check for duplicates after cleaning
CALL CheckDuplicatesInEvents();
-- Result: no more duplicates
-- SELECT COUNT(*) AS row_count FROM events;
-- The number of rows has changed from 754329 (after deleting missing country rows) to 715095.
    
-- Re-enable safe update mode
SET SQL_SAFE_UPDATES = 1;

-- ---------------------------------------------------
-- ETL PIPELINE: Transformation
-- DATA TRANSFORMATION: Create dimension tables
-- ---------------------------------------------------

-- Users dimension
-- There are users who might not have any events so it is useful to check them.
-- Calculate the proportion of users without events
SELECT 
    (SELECT COUNT(u.user_id) 
     FROM users u
     LEFT JOIN events e ON u.user_id = e.user_id
     WHERE e.user_id IS NULL) AS users_without_events,
    (SELECT COUNT(*) 
     FROM users) AS total_users,
    (SELECT COUNT(u.user_id) 
     FROM users u
     LEFT JOIN events e ON u.user_id = e.user_id
     WHERE e.user_id IS NULL) / COUNT(*) * 100 AS proportion_without_events
FROM users;
-- Result: the proportion of users with no events is 94.5583
-- Since this proportion is huge, it is better to keep all the users.

-- Create 'users' dimension table with adding 'country' and 'device'.
-- These are taken according to their latest event depicted in 'events' table.
DROP TABLE IF EXISTS dim_users;
CREATE TABLE dim_users (
    user_id INT PRIMARY KEY,
    ltv DECIMAL(10, 2),
    date DATETIME,
    country VARCHAR(255),
    device VARCHAR(255)
);
INSERT INTO dim_users (user_id, ltv, date, country, device)
SELECT
    u.user_id,
    u.ltv,
    u.date,
    -- Get the country and device from the most recent event
    le.country,
    le.device
FROM
    users u
LEFT JOIN (
    SELECT 
        user_id,
        country,
        device,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date DESC) AS rn
    FROM events
) le ON u.user_id = le.user_id AND le.rn = 1;
-- Need to note that some users might have been depicted with a country/device different from their purchase.
-- This is possible if they purchased first;
-- and only after that, they might have had another activity from a different device after their purchase.

-- Items dimension
DROP TABLE IF EXISTS dim_item;
CREATE TABLE dim_item (
    item_id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(100),
    brand VARCHAR(100),
    price_in_usd DECIMAL(10, 2)
);
-- Populate dim_item from raw `items` data
INSERT INTO dim_item (item_id, name, category, brand, price_in_usd)
SELECT 
    item_id AS item_id,
    name,
    category,
    brand,
    price_in_usd
FROM items;
-- So this is almost the original 'items' table, but without the 'variant' column.

-- Date dimension
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id DATETIME PRIMARY KEY,   -- Unique date identifier (not necessarily unique across rows)
    year INT,
    month INT,
    day INT,
    day_of_week VARCHAR(20)
);

-- Insert data from 'date' table
INSERT INTO dim_date (date_id, year, month, day, day_of_week)
SELECT DISTINCT
    date AS date_id,
    YEAR(date) AS year,
    MONTH(date) AS month,
    DAY(date) AS day,
    DAYNAME(date) AS day_of_week
FROM events;

-- ---------------------------------------------------------------
-- ETL PIPELINE: Transformation & Loading
-- DATA TRANSFORMATION: Create a fact table & insert data in it
-- ---------------------------------------------------------------

-- Create a fact table with necessary transformations
DROP TABLE IF EXISTS factSales;
CREATE TABLE factsales (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    item_id INT NOT NULL,
    price_in_usd DECIMAL(10, 2) NOT NULL,
    brand VARCHAR(255),
    category VARCHAR(255),
	device VARCHAR(50),
    country VARCHAR(50),
    ltv DECIMAL(10, 2),
    ga_session_id INT,
    date_id DATETIME,  -- Reference to the date_id from dim_date
    month INT,
	day INT, 
    day_of_week VARCHAR(20), 
    FOREIGN KEY (user_id) REFERENCES dim_users(user_id), -- FK to dim_users table
    FOREIGN KEY (item_id) REFERENCES dim_item(item_id),  -- FK to dim_item table
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)   -- FK to dim_date table
);


-- Join the events table
-- with dim_users, dim_item, and dim_date to populate the factsales table.
INSERT INTO factsales (
    user_id,
    item_id,
    price_in_usd,
    brand,
    category,
    device,
    country,
    ltv,
    ga_session_id,
    date_id,
    month,
    day,
    day_of_week
)
SELECT 
    e.user_id,
    e.item_id,
    i.price_in_usd,
    i.brand,
    i.category,
    e.device,
    e.country,
    u.ltv,
    e.ga_session_id,
    d.date_id,  -- Reference to the date_id from dim_date
    MONTH(e.date) AS month,    -- Extract the month from the event date
    DAY(e.date) AS day,        -- Extract the day from the event date
    DAYNAME(e.date) AS day_of_week -- Extract the day of the week from the event date
FROM events e
JOIN items i ON e.item_id = i.item_id
LEFT JOIN dim_date d ON e.date = d.date_id  -- Linking to the dim_date table
LEFT JOIN dim_users u ON e.user_id = u.user_id
WHERE e.type = 'purchase';  -- Only include purchase events
-- Result: 14632 rows - all are purchases
-- SELECT * from factSales;

-- ---------------------------------------------------------------
-- Creating Views as Data Marts
-- ---------------------------------------------------------------

-- View 1: Customer purchase summary by device and country
-- This might be helpful for understanding user demographics and preferences
-- which can be used in marketing & targeting in the future.
DROP VIEW IF EXISTS CustomerPurchaseSummary;
CREATE VIEW CustomerPurchaseSummary AS
SELECT 
    f.user_id,
    f.country,
    f.device,
    COUNT(DISTINCT f.ga_session_id) AS total_sessions,
    SUM(f.price_in_usd) AS total_revenue,
    AVG(f.price_in_usd) AS avg_purchase_value
FROM factsales AS f
WHERE f.ga_session_id IS NOT NULL
GROUP BY f.user_id, f.country, f.device
ORDER BY total_revenue DESC;

-- Opening the VIEW 1
SELECT * FROM CustomerPurchaseSummary;
-- Among the most active buyers majority comes from the US, and using desktop device is more popular among them.

-- View 2: Sales summary by item and category
-- This is helpful to see the most popular products,
-- to identify which price range products are best-sold.
-- Thus, this product performance can be used for marketing, pricing strategy optimization.
DROP VIEW if exists SalesByItemCategory;
CREATE VIEW SalesByItemCategory AS
SELECT 
    i.category,
    i.name,
    i.brand,
    COUNT(f.ga_session_id) AS purchase_count,
    SUM(f.price_in_usd) AS total_revenue,
    AVG(f.price_in_usd) AS avg_item_price
FROM factSales AS f
JOIN dim_item AS i ON f.item_id = i.item_id
WHERE f.ga_session_id IS NOT NULL
GROUP BY i.category, i.name, i.brand
ORDER BY purchase_count DESC; 
-- Sort by purchase count in descending order (most popular first)
-- Top 3 most sold products are joggers, hoodie and sweatshirt items.

-- Opening the VIEW 2
SELECT * FROM SalesByItemCategory;

-- View 3: User lifetime value analysis
-- This is helpful for observing purchase behavior which can be useful for marketing, targeting users.
DROP VIEW IF EXISTS UserLTVSummary;
CREATE VIEW UserLTVSummary AS
SELECT 
    u.user_id,
    u.ltv AS lifetime_value,
    COUNT(f.ga_session_id) AS total_sessions,
    SUM(f.price_in_usd) AS total_revenue,
    AVG(f.price_in_usd) AS avg_purchase_value
FROM dim_users AS u
LEFT JOIN factsales AS f ON u.user_id = f.user_id
LEFT JOIN dim_item AS i ON f.item_id = i.item_id
WHERE f.ga_session_id IS NOT NULL
GROUP BY u.user_id, u.ltv;

-- Opening the VIEW 3
SELECT * FROM UserLTVSummary;

-- ---------------------------------------------------------------
-- Create Triggers
-- ---------------------------------------------------------------

-- This trigger should be activated if there is new data in events table.
-- 'factSales' gets data from 'events' table,
-- so changes in 'events' should lead to changes in 'factSales'.

DELIMITER $$

DROP TRIGGER IF EXISTS after_purchase_insert;
CREATE TRIGGER after_purchase_insert
AFTER INSERT ON events
FOR EACH ROW
BEGIN
    IF NEW.type = 'purchase' THEN
        -- Insert into factSales table after a new purchase event
        INSERT INTO factSales (
            user_id,
            item_id,
            price_in_usd,
            brand,
            category,
            device,
            country,
            ltv,
            ga_session_id,
            date_id,
            month,
            day,
            day_of_week
        )
        SELECT 
            NEW.user_id,
            NEW.item_id,
            i.price_in_usd,
            i.brand,
            i.category,
            NEW.device,
            NEW.country,
            u.ltv,
            NEW.ga_session_id,
            CURDATE() AS date_id,  -- Current date as reference
            MONTH(NEW.date) AS month,
            DAY(NEW.date) AS day,
            DAYNAME(NEW.date) AS day_of_week
        FROM items i
        LEFT JOIN dim_users u ON NEW.user_id = u.user_id
        WHERE i.item_id = NEW.item_id;
    END IF;
END$$

DELIMITER ;

-- This trigger should be activated to change dim_dates table
-- if there is new data in events table.

DROP TRIGGER IF EXISTS after_event_insert;

DELIMITER $$
CREATE TRIGGER after_event_insert
AFTER INSERT ON events
FOR EACH ROW
BEGIN
    -- Check if the date_id exists in the dim_date table
    IF NOT EXISTS (SELECT 1 FROM dim_date WHERE date_id = NEW.date) THEN
        -- If it doesn't exist, insert the new date into dim_date
        INSERT INTO dim_date (date_id) VALUES (NEW.date);
        

    END IF;
END$$

DELIMITER ;