{{
    config(
        materialized='table'
    )
}}

WITH beers AS (
    SELECT * FROM {{ ref('silver_beers') }}
),

breweries AS (
    SELECT * FROM {{ ref('silver_breweries') }}
),

reviews AS (
    SELECT * FROM {{ ref('silver_beer_reviews') }}
),

country_lookup AS (
    SELECT * FROM {{ source('raw_data', 'iso_country_codes') }}
),

-- Calculate review statistics per beer
review_stats AS (
    SELECT
        beer_id,
        COUNT(*) AS review_count,
        AVG(score) AS avg_rating,
        STDDEV(score) AS rating_stddev,
        AVG(look) AS avg_look,
        AVG(smell) AS avg_smell,
        AVG(taste) AS avg_taste,
        AVG(feel) AS avg_feel,
        AVG(overall) AS avg_overall,
        MAX(date) AS most_recent_review_date,
        MIN(date) AS first_review_date
    FROM reviews
    GROUP BY beer_id
),

-- Calculate popularity percentiles for scoring
popularity_percentiles AS (
    SELECT
        APPROX_QUANTILES(review_count, 100) AS percentiles
    FROM review_stats
),

enriched_beers AS (
    SELECT
        -- Beer core fields
        b.id,
        b.name,
        b.style,
        b.availability,
        b.abv,
        b.retired,
        
        -- Brewery information
        b.brewery_id,
        br.name AS brewery_name,
        br.city AS brewery_city,
        br.state AS brewery_state,
        br.types AS brewery_type,
        
        -- Location with full country name
        b.state,
        b.country AS country_code,
        COALESCE(cl.english_name, b.country) AS country_name,
        
        -- Review statistics
        COALESCE(rs.review_count, 0) AS review_count,
        ROUND(rs.avg_rating, 2) AS avg_rating,
        ROUND(rs.rating_stddev, 2) AS rating_stddev,
        ROUND(rs.avg_look, 2) AS avg_look,
        ROUND(rs.avg_smell, 2) AS avg_smell,
        ROUND(rs.avg_taste, 2) AS avg_taste,
        ROUND(rs.avg_feel, 2) AS avg_feel,
        ROUND(rs.avg_overall, 2) AS avg_overall,
        rs.most_recent_review_date,
        rs.first_review_date,
        
        -- Popularity score (1-10 scale based on percentile of review count)
        CASE 
            WHEN rs.review_count IS NULL THEN 1
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(10)] FROM popularity_percentiles) THEN 1
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(20)] FROM popularity_percentiles) THEN 2
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(30)] FROM popularity_percentiles) THEN 3
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(40)] FROM popularity_percentiles) THEN 4
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(50)] FROM popularity_percentiles) THEN 5
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(60)] FROM popularity_percentiles) THEN 6
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(70)] FROM popularity_percentiles) THEN 7
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(80)] FROM popularity_percentiles) THEN 8
            WHEN rs.review_count <= (SELECT percentiles[OFFSET(90)] FROM popularity_percentiles) THEN 9
            ELSE 10
        END AS popularity_score,
        
        -- Derived flags
        CASE WHEN rs.review_count >= 100 THEN TRUE ELSE FALSE END AS is_highly_reviewed,
        CASE WHEN rs.avg_rating >= 4.0 THEN TRUE ELSE FALSE END AS is_highly_rated,
        CASE WHEN rs.rating_stddev < 0.5 THEN TRUE ELSE FALSE END AS has_consistent_ratings
        
    FROM beers b
    LEFT JOIN breweries br ON b.brewery_id = br.id
    LEFT JOIN review_stats rs ON b.id = rs.beer_id
    LEFT JOIN country_lookup cl ON UPPER(b.country) = UPPER(cl.alpha_2)
)

SELECT * FROM enriched_beers