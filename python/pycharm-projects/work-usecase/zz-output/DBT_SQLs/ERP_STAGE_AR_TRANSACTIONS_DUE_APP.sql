{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}

SELECT
            PAYMENT_SCHEDULE_ID, RECEIVABLE_APPLICATION_ID, GL_DATE_AR, RECEIPT_DATE, AMOUNT_APPLIED,
            DISPLAY_AR, LAST_UPDATE_DATE, APPLICATION_TYPE, STATUS
        FROM
       {{ref('ERP_STAGE_AR_TRANSACTIONS_DUE')}}
        where RECEIVABLE_APPLICATION_ID <> 0