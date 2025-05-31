{%- set trimmed_source_rel = model.name | replace("STREAM_","") -%}
{%- set src_tbl = "STREAM_S" if is_incremental() else "STREAM" -%}

{{
    config(
        alias=trimmed_source_rel,
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='ORDER_REF_ID',
        enabled=run_flag_env()
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('EVENT_STREAM_PROD', src_tbl|string) }}
), JSON_FLATTEN AS (
    SELECT
        s.RECORD_CONTENT:event_id::STRING AS STREAM_ID,
        s.RECORD_CONTENT:timestamp::TIMESTAMP_NTZ AS STREAM_TIMESTAMP,
        s.RECORD_CONTENT:data:total::FLOAT AS TOTAL,
        s.RECORD_CONTENT:data:group_id::STRING AS GROUP_ID,
        s.RECORD_CONTENT:data:unit::STRING AS UNIT,
        s.RECORD_CONTENT:data:program_id::STRING AS PROGRAM_ID,
        s.RECORD_CONTENT:data:client_name::STRING AS CLIENT_NAME,
        s.RECORD_CONTENT:data:order_ref_id::STRING AS ORDER_REF_ID,
        s.RECORD_CONTENT:data:sequence::NUMBER AS SEQUENCE,
        s.RECORD_CONTENT:updated_by::STRING AS UPDATED_BY,
        s.RECORD_CONTENT:origin::STRING AS ORIGIN,
        s.RECORD_CONTENT:topic::STRING AS TOPIC,
        s.RECORD_CONTENT:category::STRING AS CATEGORY
    FROM source AS s
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_REF_ID ORDER BY STREAM_TIMESTAMP DESC) = 1
)
SELECT *
FROM JSON_FLATTEN
