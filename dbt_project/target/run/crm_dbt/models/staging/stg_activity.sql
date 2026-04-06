

  create or replace view `crm_staging`.`stg_activity` 
  
    
  
  
    
    
  as (
    -- stg_activity.sql

WITH source AS (
    SELECT activity_id,
           type,
           assigned_to_user,
           deal_id,
           done,
           due_to
    FROM crm.lnd_activity
)

SELECT source.activity_id                              AS activity_id,
       trim(source.type)                               AS activity_type,
       source.assigned_to_user                         AS assigned_to_user_id,
       source.deal_id                                  AS deal_id,
       CASE
           WHEN source.done = 1
               THEN TRUE
           ELSE FALSE
       END                                             AS is_done,
       CASE
           WHEN source.due_to = '' OR source.due_to IS NULL
               THEN NULL
           ELSE parseDateTimeBestEffort(source.due_to)
       END                                             AS due_at
FROM source
WHERE source.activity_id IS NOT NULL








    
  )
      
      
                    -- end_of_sql
                    
                    