-- Sample data for local development, loaded with `make db-seed` (after `make db-upgrade`).
-- Idempotent: re-running it updates the rows to the values below.

-- Triggers the dummy DAG in docker/airflow/dags/local_dummy_dag.py
INSERT INTO process (
    organization_sgid,
    organization_unit_sgid,
    dataset_sgid,
    sgid,
    name,
    description,
    dag_id,
    user_parameter_schema,
    internal_config,
    parameter_mapping
) VALUES (
    'org_developers',
    'default',
    'dat_some-local-dataset',
    'prc_developers-some-local-dataset-dummy',
    'Dummy Process',
    'Example process to use during local development',
    'developers-some_local_dataset_dummy',
    '{
        "type": "object",
        "properties": {
            "file": {"type": "string"}
        },
        "required": ["file"]
    }',
    '{}',
    '{"file": "file"}'
)
ON CONFLICT (sgid) DO UPDATE SET
    organization_sgid = excluded.organization_sgid,
    organization_unit_sgid = excluded.organization_unit_sgid,
    dataset_sgid = excluded.dataset_sgid,
    name = excluded.name,
    description = excluded.description,
    dag_id = excluded.dag_id,
    user_parameter_schema = excluded.user_parameter_schema,
    internal_config = excluded.internal_config,
    parameter_mapping = excluded.parameter_mapping,
    updated_at = now()
RETURNING id, sgid, created_at, updated_at;
