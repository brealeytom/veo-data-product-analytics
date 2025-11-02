{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'beer_reviews') }}
),

cleaned_beers AS (
    SELECT id
    FROM {{ ref('silver_beers') }}
),

cleaned_reviews AS (
    SELECT
        beer_id,
        username,
        date,
        text,
        look,
        smell,
        taste,
        feel,
        overall,
        score
    FROM source_data
    WHERE EXISTS (
        SELECT 1 
        FROM cleaned_beers 
        WHERE cleaned_beers.id = source_data.beer_id
    )
)

SELECT * FROM cleaned_reviews