-- stg_stages.sql

WITH source AS (
    SELECT stage_id,
           stage_name
    FROM crm.lnd_stages
),

cleaned AS (
    SELECT stage_id            AS stage_id,
           trim(stage_name)    AS stage_name
    FROM source
    WHERE stage_id IS NOT NULL
)


SELECT stage_id,
       stage_name
FROM cleaned