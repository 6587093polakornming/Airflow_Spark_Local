SELECT
  m.title,

  -- Genres as string
  STRING_AGG(DISTINCT g.genre_name, ', ') AS genres,

  -- Keywords as string
  STRING_AGG(DISTINCT k.keyword_name, ', ') AS keywords,

  m.overview,
FROM `datapipeline467803.tmdb_dw.dim_movie` m
LEFT JOIN `datapipeline467803.tmdb_dw.bridge_movie_genre` bmg
  USING(movie_id)
LEFT JOIN `datapipeline467803.tmdb_dw.dim_genre` g
  USING(genre_id)
LEFT JOIN `datapipeline467803.tmdb_dw.bridge_movie_keyword` bmk
  USING(movie_id)
LEFT JOIN `datapipeline467803.tmdb_dw.dim_keyword` k
  USING(keyword_id)
LEFT JOIN `datapipeline467803.tmdb_dw.bridge_movie_company` bmc
  USING(movie_id)
LEFT JOIN `datapipeline467803.tmdb_dw.dim_production_company` c
  USING(company_id)
LEFT JOIN `datapipeline467803.tmdb_dw.bridge_movie_language` bml
  USING(movie_id)
LEFT JOIN `datapipeline467803.tmdb_dw.dim_spoken_language` l
  USING(language_id)
LEFT JOIN `datapipeline467803.tmdb_dw.bridge_movie_country` bmcountry
  USING(movie_id)
LEFT JOIN `datapipeline467803.tmdb_dw.dim_production_country` co
  USING(country_id)

GROUP BY
  m.title,
  m.overview
;