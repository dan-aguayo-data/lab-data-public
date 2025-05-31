{%- set trimmed_source_rel = model.name | replace("BRONZE_","") -%}

{{  
    config(
        alias=trimmed_source_rel
    )
}}

WITH source AS (
    SELECT DOC_NAME, DOC_ROW_NUM, INGESTED_TIMESTAMP, RECORD AS DATA
    FROM {{ source('DATA_PIPE', 'EVENTS_BRONZE') }}
    WHERE 
    DOC_NAME = (
        SELECT DOC_NAME 
        FROM {{ source('DATA_PIPE', 'EVENTS_BRONZE') }}
        WHERE LOWER(DOC_NAME) LIKE LOWER('prod/events/load/%')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY 'EVENTS' ORDER BY INGESTED_TIMESTAMP DESC) = 1
    )
)
SELECT 
    s.data:event_id::BIGINT AS EVENT_ID,
    s.data:site_id::BIGINT AS SITE_ID,
    s.data:program_code::STRING AS PROGRAM_CODE,
    s.data:user_email::STRING AS USER_EMAIL,
    s.data:given_name::STRING AS GIVEN_NAME,
    s.data:family_name::STRING AS FAMILY_NAME,
    s.data:phone::STRING AS PHONE,
    s.data:residence::STRING AS RESIDENCE,
    s.data:residence_type::STRING AS RESIDENCE_TYPE,
    s.data:apartment::STRING AS APARTMENT,
    s.data:road::STRING AS ROAD,
    s.data:city::STRING AS CITY,
    s.data:zip_code::STRING AS ZIP_CODE,
    s.data:dropoff_point::STRING AS DROPOFF_POINT,
    NULLIF(s.data:event_date,'')::DATE AS EVENT_DATE,
    s.data:event_time::STRING AS EVENT_TIME,
    s.data:items::BIGINT AS ITEMS,
    s.data:transaction_type::STRING AS TRANSACTION_TYPE,
    s.data:delivery_status::STRING AS DELIVERY_STATUS,
    s.data:items_received::BIGINT AS ITEMS_RECEIVED,
    s.data:items_lost::BIGINT AS ITEMS_LOST,
    s.data:order_channel::STRING AS ORDER_CHANNEL,
    s.data:packages_received::STRING AS PACKAGES_RECEIVED,
    s.data:comments::STRING AS COMMENTS,
    NULLIF(s.data:archived_at,'')::TIMESTAMP_NTZ AS ARCHIVED_AT,
    NULLIF(s.data:recorded_at,'')::TIMESTAMP_NTZ AS RECORDED_AT,
    NULLIF(s.data:modified_at,'')::TIMESTAMP_NTZ AS MODIFIED_AT
FROM source AS s