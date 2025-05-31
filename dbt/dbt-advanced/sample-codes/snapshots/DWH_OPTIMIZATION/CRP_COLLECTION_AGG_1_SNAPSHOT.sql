{% snapshot crp_collection_agg_1_snapshot %}
  {{
    config(
      strategy='check',
      unique_key='INCREMENTAL_ID',
      invalidate_hard_deletes=True,
      check_cols=[
        'ID',
        'MULTI_SCHEME_ID',
        'SCHEME',
        'SITE_NUMBER',
        'CONSUMER_ID',
        'TRANSACTION_DATE_KEY',
        'PAYMENT_METHOD',
        'SCHEME_CONSUMER_FLAG',
        'TRANSACTION_BANDING',
        'TRANSACTION_POSTCODE',
        'CONSUMER_TYPE',
        'MATERIAL_TYPE_ID',
        'POS_DEVICE_NAME',
        'TOTAL_TRANSACTIONS',
        'TOTAL_QTY',
        'TOTAL_GROSS_AMOUNT',
        'TOTAL_GST_AMOUNT',
        'TOTAL_TAXABLE_AMOUNT',
      ]
    )
  }}

  select 
    INCREMENTAL_ID,
    MULTI_SCHEME_ID, 
    SCHEME, 
    ID, 
    SITE_NUMBER, 
    CONSUMER_ID, 
    TRANSACTION_DATE_KEY, 
    PAYMENT_METHOD, 
    SCHEME_CONSUMER_FLAG, 
    TRANSACTION_BANDING, 
    TRANSACTION_POSTCODE, 
    CONSUMER_TYPE, 
    MATERIAL_TYPE_ID, 
    POS_DEVICE_NAME, 
    TOTAL_TRANSACTIONS, 
    TOTAL_QTY, 
    TOTAL_GROSS_AMOUNT, 
    TOTAL_GST_AMOUNT, 
    TOTAL_TAXABLE_AMOUNT, 
    INSERT_DATE_UTC
  from COMMON_PROD.DWH.CRP_COLLECTION_AGG_1

{% endsnapshot %}
