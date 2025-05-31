{% snapshot braze_goal_progress_snap %}

    {{
        config(
          strategy='check',
          unique_key='"goal_d_id"',
          check_cols=['"goal_progress_banding"']
        )
    }}

    select * from {{ ref('BRAZE_GOAL_PROGRESS') }} where "qty_returned_towards_goal" > 0

{% endsnapshot %}