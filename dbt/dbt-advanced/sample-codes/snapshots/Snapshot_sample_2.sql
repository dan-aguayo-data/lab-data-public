{% snapshot order_summary_snapSHOT %}
  {{
    config(
      strategy='check',
      unique_key='EVENT_ID',
      invalidate_hard_deletes=True,
      check_cols=[
        'RECORD_ID',
        'PROGRAM_CODE',
        'PROGRAM',
        'DEPOT_CODE',
        'CLIENT_ID',
        'ORDER_DATE_KEY',
        'PAYMENT_TYPE',
        'PROGRAM_CLIENT_FLAG',
        'ORDER_RANGE',
        'ORDER_REGION',
        'CLIENT_TYPE',
        'ITEM_CATEGORY_ID',
        'TERMINAL_NAME',
        'TOTAL_ORDERS',
        'TOTAL_QUANTITY',
        'TOTAL_VALUE',
        'TOTAL_TAX',
        'TOTAL_NET_VALUE'
      ]
    )
  }}

  SELECT 
    EVENT_ID,
    PROGRAM_CODE,
    PROGRAM,
    RECORD_ID,
    DEPOT_CODE,
    CLIENT_ID,
    ORDER_DATE_KEY,
    PAYMENT_TYPE,
    PROGRAM_CLIENT_FLAG,
    ORDER_RANGE,
    ORDER_REGION,
    CLIENT_TYPE,
    ITEM_CATEGORY_ID,
    TERMINAL_NAME,
    TOTAL_ORDERS,
    TOTAL_QUANTITY,
    TOTAL_VALUE,
    TOTAL_TAX,
    TOTAL_NET_VALUE,
    LOAD_TIMESTAMP_UTC
  FROM DATA_HUB.WAREHOUSE.ORDER_AGG_SUMMARY

{% endsnapshot %}