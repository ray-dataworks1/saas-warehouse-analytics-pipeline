{{ config(materialized='table',
          schema='core') }}

-- fct_support_ticket
-- Grain: 1 row per ticket_id
-- Purpose: core support ticket fact for joins and SLA/CSAT marts

select
  ticket_id,
  account_id,
  submitted_at,
  closed_at,
  resolution_time_hours,
  priority_level,
  first_response_time_minutes,
  satisfaction_score,
  escalation_flag
from {{ ref('stg_support_tickets') }}