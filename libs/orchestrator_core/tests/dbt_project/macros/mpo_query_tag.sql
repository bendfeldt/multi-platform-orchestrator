{#- Copy into the macros/ folder of every dbt project that runs on Snowflake.

    Adds the model to the query tag Dagster sets for the dbt step (MPO_QUERY_TAG, see
    orchestrator_core/query_tag.py), so Snowflake credits can be attributed per model:
    {"mpo_run":"...","mpo_step":"...","mpo_dbt_node":"model.<project>.<name>"}.
    Overrides dbt-snowflake's snowflake__set_query_tag / snowflake__unset_query_tag, which
    dbt dispatches to the root project first. A model's own `query_tag` config still wins. -#}

{% macro snowflake__set_query_tag() -%}
  {%- set new_query_tag = config.get('query_tag') -%}
  {%- set mpo_tag = env_var('MPO_QUERY_TAG', '') -%}
  {%- if not new_query_tag and mpo_tag and model is defined and model -%}
    {%- set new_query_tag = mpo_tag[:-1] ~ ',"mpo_dbt_node":"' ~ model.unique_id ~ '"}' -%}
  {%- endif -%}
  {%- if new_query_tag -%}
    {%- set original_query_tag = get_current_query_tag() -%}
    {%- do run_query("alter session set query_tag = '{}'".format(new_query_tag)) -%}
    {{ return(original_query_tag) }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}

{% macro snowflake__unset_query_tag(original_query_tag) -%}
  {#- Same condition as above: only undo what set_query_tag did. -#}
  {%- if config.get('query_tag') or (env_var('MPO_QUERY_TAG', '') and model is defined and model) -%}
    {%- if original_query_tag -%}
      {%- do run_query("alter session set query_tag = '{}'".format(original_query_tag)) -%}
    {%- else -%}
      {%- do run_query("alter session unset query_tag") -%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
