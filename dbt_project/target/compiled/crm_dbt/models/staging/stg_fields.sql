-- stg_fields.sql

WITH source AS (
    SELECT id,
           field_key,
           name,
           field_value_options
    FROM crm.lnd_fields
),

cleaned AS (
    SELECT id                          AS field_id,
           trim(lower(field_key))      AS field_key,
           trim(name)                  AS field_name,
           field_value_options         AS field_value_options
    FROM source
    WHERE id IS NOT NULL
)


SELECT field_id,
       field_key,
       field_name,
       field_value_options
FROM cleaned