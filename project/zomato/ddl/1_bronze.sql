-- =====================================================================
-- Phase 2 · Step 4 — RAW tables (Path 1 batch, manual plain-CSV upload)
-- Databricks / Unity Catalog: catalog = zomato, schema = bronze
-- Column ORDER matches the CSV files you upload, with headers skipped.
-- NOTE: the four DIMENSION files (restaurant/users/food/menu) carry a leading
-- unnamed index column, so their tables start with a throwaway `_idx` column.
-- The fact files (orders/order_items/reviews) have no index column.
-- =====================================================================
CREATE CATALOG IF NOT EXISTS zomato;
USE CATALOG zomato;

CREATE SCHEMA IF NOT EXISTS bronze;
USE SCHEMA bronze;

-- restaurant.csv  (index col dropped):
-- id,name,city,rating,rating_count,cost,cuisine,lic_no,link,address,menu
CREATE OR REPLACE TABLE zomato.bronze.restaurants (
  _idx          STRING COMMENT 'leading index column in the CSV (ignored downstream)',
  id            STRING,
  name          STRING,
  city          STRING,
  rating        STRING,
  rating_count  STRING,
  cost          STRING,
  cuisine       STRING,
  lic_no        STRING,
  link          STRING,
  address       STRING,
  menu          STRING
) USING DELTA;

-- users.csv: user_id,name,email,password,Age,Gender,Marital Status,
--            Occupation,Monthly Income,Educational Qualifications,Family size
CREATE OR REPLACE TABLE zomato.bronze.users (
  _idx            STRING COMMENT 'leading index column in the CSV',
  user_id         STRING,
  name            STRING,
  email           STRING,
  password        STRING,
  age             STRING,
  gender          STRING,
  marital_status  STRING,
  occupation      STRING,
  monthly_income  STRING,
  education       STRING,
  family_size     STRING
) USING DELTA;

-- food.csv: f_id,item,veg_or_non_veg
CREATE OR REPLACE TABLE zomato.bronze.food (
  _idx           STRING COMMENT 'leading index column in the CSV',
  f_id           STRING,
  item           STRING,
  veg_or_non_veg STRING
) USING DELTA;

-- menu.csv: ,menu_id,r_id,f_id,cuisine,price
CREATE OR REPLACE TABLE zomato.bronze.menu (
  _idx     STRING COMMENT 'leading index column in the CSV',
  menu_id  STRING,
  r_id     STRING,
  f_id     STRING,
  cuisine  STRING,
  price    STRING
) USING DELTA;

-- generated/orders.csv (clean, typed):
CREATE OR REPLACE TABLE zomato.bronze.orders (
  order_id          BIGINT,
  order_timestamp   TIMESTAMP_NTZ,
  order_date        DATE,
  user_id           BIGINT,
  r_id              BIGINT,
  restaurant_city   STRING,
  cuisine           STRING,
  items_count       INT,
  sales_qty         INT,
  subtotal          DECIMAL(12,2),
  discount          DECIMAL(12,2),
  delivery_fee      DECIMAL(12,2),
  gst               DECIMAL(12,2),
  sales_amount      DECIMAL(12,2),
  currency          STRING,
  payment_method    STRING,
  order_status      STRING,
  customer_rating   DECIMAL(3,2),
  delivery_time_min INT
) USING DELTA;

-- generated/order_items.csv (clean, typed):
CREATE OR REPLACE TABLE zomato.bronze.order_items (
  order_item_id BIGINT,
  order_id      BIGINT,
  r_id          BIGINT,
  f_id          STRING,
  price         DECIMAL(12,2),
  quantity      INT,
  line_amount   DECIMAL(12,2)
) USING DELTA;

-- generated/reviews.csv (clean, typed) — free text for the AI layer:
CREATE OR REPLACE TABLE zomato.bronze.reviews (
  review_id     BIGINT,
  order_id      BIGINT,
  user_id       BIGINT,
  restaurant_id BIGINT,
  rating        DECIMAL(3,2),
  comment       STRING,
  review_date   DATE
) USING DELTA;
