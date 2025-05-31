{% snapshot ext_braze_goal_progress_snap %}

--********* UAT *********--
{% if target.name.upper() != 'PROD' %}

    {{
        config(
          strategy='check',
          unique_key='"id"',
          check_cols=['"goal_progress_banding"']
        )
    }}

{%- else -%}

    {{
        config(
          strategy='check',
          unique_key='"goal_d_id"',
          check_cols=['"goal_progress_banding"']
        )
    }}

{%- endif -%}
--********* UAT *********--


    select * from {{ ref('EXT_BRAZE_GOAL_PROGRESS') }} where "qty_returned_towards_goal" > 0

{% endsnapshot %}