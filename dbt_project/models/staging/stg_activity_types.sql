-- stg_activity_types.sql

WITH source AS (
    SELECT id,
           name,
           active,
           type
    FROM crm.lnd_activity_types
)


SELECT source.id                                       AS activity_type_id,
       trim(source.name)                               AS activity_type_name,
       CASE
           WHEN lower(trim(source.active)) = 'yes'
               THEN TRUE
           ELSE FALSE
       END                                             AS is_active,
       trim(lower(source.type))                        AS type_key
FROM source
WHERE source.id IS NOT NULL