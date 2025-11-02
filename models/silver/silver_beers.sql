{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'beers') }}
),

cleaned_breweries AS (
    SELECT id
    FROM {{ ref('silver_breweries') }}
),

cleaned_beers AS (
    SELECT
        id,
        name,
        brewery_id,
        state,
        country,
        style,
        TRIM(availability) AS availability,
        abv,
        notes,
        retired
    FROM source_data
    WHERE abv < 20
        AND (LOWER(notes) NOT LIKE '%duplicate%' 
             OR name IN ('Portsmouth Oktoberfest', 'Saint Of Circumstance Sour Mash Whiskey Barrel Aged IPA'))
        AND EXISTS (
            SELECT 1 
            FROM cleaned_breweries 
            WHERE cleaned_breweries.id = source_data.brewery_id
        )
)

SELECT * FROM cleaned_beers