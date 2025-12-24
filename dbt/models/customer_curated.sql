{{ config(materialized='table') }}

select *
from practice_db.raw.customer