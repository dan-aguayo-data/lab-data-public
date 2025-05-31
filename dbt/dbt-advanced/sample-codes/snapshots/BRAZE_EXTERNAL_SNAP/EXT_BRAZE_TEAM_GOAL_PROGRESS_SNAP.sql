{% snapshot ext_braze_team_goal_progress_snap %}

    {{
        config(
          strategy='check',
          unique_key='"id"',
          check_cols=['"team_goal_progress_banding"'],
          enabled = run_flag_excl_prod()
        )
    }}

    select * from {{ ref('EXT_BRAZE_TEAM_GOAL_PROGRESS') }} where "team_qty_returned_towards_goal" > 0

{% endsnapshot %}