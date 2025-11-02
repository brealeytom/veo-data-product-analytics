{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'breweries') }}
),

cleaned_breweries AS (
    SELECT
        id,
        name,
        city,
        state,
        country,
        notes,
        types
    FROM source_data
    WHERE notes NOT LIKE 'Duplicate of %' 
        AND notes NOT LIKE 'Duplicate listing'
)

SELECT * FROM cleaned_breweries