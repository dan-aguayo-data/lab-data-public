{% snapshot stream_event_progress_snapshot %}

--********* TEST *********--
{% if target.name.upper() != 'PROD' %}

    {{
        config(
          strategy='check',
          unique_key='"event_id"',
          check_cols=['"progress_level"']
        )
    }}

{%- else -%}

    {{
        config(
          strategy='check',
          unique_key='"event_ref_id"',
          check_cols=['"progress_level"']
        )
    }}

{%- endif -%}
--********* TEST *********--

    SELECT * 
    FROM {{ ref('STREAM_EVENT_PROGRESS') }} 
    WHERE "items_processed" > 0

{% endsnapshot %}