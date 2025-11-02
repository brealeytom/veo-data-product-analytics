{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'beers') }}
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
)

SELECT * FROM cleaned_beers