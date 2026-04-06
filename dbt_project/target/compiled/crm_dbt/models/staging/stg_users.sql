-- stg_users.sql

WITH source AS (
    SELECT id,
           name,
           email,
           modified
    FROM crm.lnd_users
),

cleaned AS (
    SELECT id                                    AS user_id,
           trim(name)                            AS full_name,
           lower(trim(email))                    AS email,
           parseDateTimeBestEffort(modified)     AS modified_at
    FROM source
    WHERE id IS NOT NULL
)


SELECT user_id,
       full_name,
       email,
       modified_at
FROM cleaned