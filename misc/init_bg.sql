CREATE SCHEMA IF NOT EXISTS `datapipeline467803.tmdb_dw`;

-- ======================
-- DIMENSION TABLES
-- ======================

-- dim_movie
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.dim_movie` (
  movie_id INT64 NOT NULL,
  title STRING,
  status STRING,
  release_date DATE,
  adult BOOL,
  original_language STRING,
  overview STRING,
  PRIMARY KEY (movie_id) NOT ENFORCED
);

-- dim_genre
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.dim_genre` (
  genre_id INT64 NOT NULL,
  genre_name STRING,
  PRIMARY KEY (genre_id) NOT ENFORCED
);

-- dim_keyword
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.dim_keyword` (
  keyword_id INT64 NOT NULL,
  keyword_name STRING,
  PRIMARY KEY (keyword_id) NOT ENFORCED
);

-- dim_production_company
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.dim_production_company` (
  company_id INT64 NOT NULL,
  company_name STRING,
  PRIMARY KEY (company_id) NOT ENFORCED
);

-- dim_spoken_language
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.dim_spoken_language` (
  language_id INT64 NOT NULL,
  language_name STRING,
  PRIMARY KEY (language_id) NOT ENFORCED
);

-- dim_production_country
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.dim_production_country` (
  country_id INT64 NOT NULL,
  country_name STRING,
  PRIMARY KEY (country_id) NOT ENFORCED
);

-- ======================
-- FACT TABLES
-- ======================

-- fact_movie (1 row per movie's performance)
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.fact_movie` (
  movie_id INT64 NOT NULL,
  vote_average FLOAT64,
  popularity FLOAT64,
  vote_count INT64,
  budget INT64,
  revenue INT64,
  runtime INT64,
  FOREIGN KEY (movie_id) REFERENCES `datapipeline467803.tmdb_dw.dim_movie` (movie_id) NOT ENFORCED
);


-- ======================
-- BRIDGE TABLES
-- ======================

-- bridge_movie_genre
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.bridge_movie_genre` (
  bridge_id INT64 NOT NULL,
  movie_id INT64 NOT NULL,
  genre_id INT64 NOT NULL,
  PRIMARY KEY (bridge_id) NOT ENFORCED,
  FOREIGN KEY (movie_id) REFERENCES `datapipeline467803.tmdb_dw.dim_movie` (movie_id) NOT ENFORCED, 
  FOREIGN KEY (genre_id) REFERENCES `datapipeline467803.tmdb_dw.dim_genre` (genre_id) NOT ENFORCED
);

-- bridge_movie_keyword
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.bridge_movie_keyword` (
  bridge_id INT64 NOT NULL,
  movie_id INT64 NOT NULL,
  keyword_id INT64 NOT NULL,
  PRIMARY KEY (bridge_id) NOT ENFORCED,
  FOREIGN KEY (movie_id) REFERENCES `datapipeline467803.tmdb_dw.dim_movie` (movie_id) NOT ENFORCED,
  FOREIGN KEY (keyword_id) REFERENCES `datapipeline467803.tmdb_dw.dim_keyword` (keyword_id) NOT ENFORCED
);

-- bridge_movie_production_company
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.bridge_movie_company` (
  bridge_id INT64 NOT NULL,
  movie_id INT64 NOT NULL,
  company_id INT64 NOT NULL,
  PRIMARY KEY (bridge_id) NOT ENFORCED,
  FOREIGN KEY (movie_id) REFERENCES `datapipeline467803.tmdb_dw.dim_movie` (movie_id) NOT ENFORCED,
  FOREIGN KEY (company_id) REFERENCES `datapipeline467803.tmdb_dw.dim_production_company` (company_id) NOT ENFORCED
);

-- bridge_movie_language
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.bridge_movie_language` (
  bridge_id INT64 NOT NULL,
  movie_id INT64 NOT NULL,
  language_id INT64 NOT NULL,
  PRIMARY KEY (bridge_id) NOT ENFORCED,
  FOREIGN KEY (movie_id) REFERENCES `datapipeline467803.tmdb_dw.dim_movie` (movie_id) NOT ENFORCED,
  FOREIGN KEY (language_id) REFERENCES `datapipeline467803.tmdb_dw.dim_spoken_language` (language_id) NOT ENFORCED
);

-- bridge_movie_country
CREATE TABLE IF NOT EXISTS `datapipeline467803.tmdb_dw.bridge_movie_country` (
  bridge_id INT64 NOT NULL,
  movie_id INT64 NOT NULL,
  country_id INT64 NOT NULL,
  PRIMARY KEY (bridge_id) NOT ENFORCED,
  FOREIGN KEY (movie_id) REFERENCES `datapipeline467803.tmdb_dw.dim_movie` (movie_id) NOT ENFORCED,
  FOREIGN KEY (country_id) REFERENCES `datapipeline467803.tmdb_dw.dim_production_country` (country_id) NOT ENFORCED
);
