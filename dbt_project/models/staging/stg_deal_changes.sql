-- stg_deal_changes.sql

WITH source AS (
    SELECT deal_id,
           change_time,
           changed_field_key,
           new_value
    FROM crm.lnd_deal_changes
),

cleaned AS (
    SELECT deal_id                                 AS deal_id,
           CASE
               WHEN change_time = '' OR change_time IS NULL
                   THEN NULL
               ELSE parseDateTimeBestEffort(change_time)
           END                                     AS change_time,
           trim(changed_field_key)                 AS changed_field_key,
           trim(new_value)                         AS new_value
    FROM source
    WHERE deal_id IS NOT NULL
)


SELECT deal_id,
       change_time,
       changed_field_key,
       new_value
FROM cleaned