USE WAREHOUSE COMPUTE_WH;

/*
TRUNCATE sales_fact;
TRUNCATE date_dim;
TRUNCATE color_dim;
TRUNCATE product_dim;
TRUNCATE product_channel_dim;
TRUNCATE label_dim;
TRUNCATE channel_dim;
TRUNCATE category_dim;
TRUNCATE product_type_dim;
TRUNCATE currency_dim;

DROP TABLE sales_fact;
DROP TABLE date_dim;
DROP TABLE color_dim;
DROP TABLE product_dim;
DROP TABLE product_channel_dim;
DROP TABLE label_dim;
DROP TABLE channel_dim;
DROP TABLE category_dim;
DROP TABLE product_type_dim;
DROP TABLE currency_dim;
*/

CREATE TABLE currency_dim
(
  id INTEGER PRIMARY KEY,
  acronym VARCHAR(3) NOT NULL
);

CREATE TABLE product_type_dim
(
  id INTEGER PRIMARY KEY,
  name VARCHAR(50) NOT NULL
);

CREATE TABLE category_dim
(
  id INTEGER PRIMARY KEY,
  name VARCHAR(50) NOT NULL,

  product_type INTEGER,
  CONSTRAINT product_type_fkey
    FOREIGN KEY (product_type)
    REFERENCES product_type_dim (id) NOT ENFORCED
);

CREATE TABLE channel_dim
(
  id INTEGER PRIMARY KEY,
  name VARCHAR(50) NOT NULL
);

CREATE TABLE label_dim
(
  id INTEGER PRIMARY KEY,
  name VARCHAR(50) NOT NULL
);

CREATE TABLE product_channel_dim
(
  channel INTEGER NOT NULL,
  CONSTRAINT channel_fkey
    FOREIGN KEY (channel)
    REFERENCES channel_dim (id) NOT ENFORCED,
  
  product VARCHAR(36) NOT NULL,
  CONSTRAINT product_fkey
    FOREIGN KEY (product)
    REFERENCES product_dim (id) NOT ENFORCED,
);

CREATE TABLE product_dim
(
  id VARCHAR(36) NOT NULL PRIMARY KEY,
  title VARCHAR,
  subtitle VARCHAR,
  short_description VARCHAR,

  discount BOOLEAN,
  rating DECIMAL(18,2),
  customizable BOOLEAN,
  extended_sizing BOOLEAN,
  gift_card BOOLEAN,
  jersey BOOLEAN,
  nba BOOLEAN,
  nfl BOOLEAN,
  sustainable BOOLEAN,
  url VARCHAR,


  label INTEGER,
  CONSTRAINT label_fkey
    FOREIGN KEY (label)
    REFERENCES label_dim (id) NOT ENFORCED,

  category INTEGER,
  CONSTRAINT category_fkey
    FOREIGN KEY (category)
    REFERENCES category_dim (id) NOT ENFORCED

);

CREATE TABLE color_dim
(
  id VARCHAR(36) NOT NULL PRIMARY KEY,
  offline_id VARCHAR(36) NOT NULL,
  short_id VARCHAR(12) NOT NULL,
  is_main BOOLEAN, -- Calculated


  description VARCHAR,
  color_num INTEGER,
  full_price INTEGER,
  current_price DECIMAL(18,2),
  in_stock BOOLEAN,
  coming_soon BOOLEAN,
  best_seller BOOLEAN,
  excluded BOOLEAN,
  launch BOOLEAN,
  member_exclusive BOOLEAN,
  pre_build_id VARCHAR(36),
  is_new BOOLEAN,
  image_url VARCHAR,

  product VARCHAR(36) NOT NULL,
  CONSTRAINT product_fkey
    FOREIGN KEY (product)
    REFERENCES product_dim (id) NOT ENFORCED,

  currency INTEGER,
  CONSTRAINT currency_fkey
    FOREIGN KEY (currency)
    REFERENCES currency_dim (id) NOT ENFORCED
);

-- SALES
CREATE TABLE date_dim
(
  id INTEGER PRIMARY KEY,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL
);

CREATE TABLE sales_fact
(
  id INTEGER PRIMARY KEY,
  sales DECIMAL(18,2) NOT NULL,
  date INTEGER,
  CONSTRAINT date_fkey
    FOREIGN KEY (date)
    REFERENCES date_dim (id) NOT ENFORCED,
  product VARCHAR(36),
  CONSTRAINT product_fkey
    FOREIGN KEY (product)
    REFERENCES product_dim (id) NOT ENFORCED
);

/*
SELECT * FROM sales_fact;
SELECT * FROM date_dim;
SELECT * FROM color_dim;
SELECT * FROM product_dim;
SELECT * FROM product_channel_dim;
SELECT * FROM label_dim;
SELECT * FROM channel_dim;
SELECT * FROM category_dim;
SELECT * FROM product_type_dim;
SELECT * FROM currency_dim;

SELECT
(SELECT COUNT(*) FROM sales_fact) AS  sales_fact,
(SELECT COUNT(*) FROM date_dim) AS date_dim,
(SELECT COUNT(*) FROM color_dim) AS color_dim,
(SELECT COUNT(*) FROM product_dim) AS product_dim,
(SELECT COUNT(*) FROM product_channel_dim) AS product_channel_dim,
(SELECT COUNT(*) FROM label_dim) AS label_dim,
(SELECT COUNT(*) FROM channel_dim) AS channel_dim,
(SELECT COUNT(*) FROM category_dim) AS category_dim,
(SELECT COUNT(*) FROM product_type_dim) AS product_type_dim,
(SELECT COUNT(*) FROM currency_dim) AS currency_dim;
*/
