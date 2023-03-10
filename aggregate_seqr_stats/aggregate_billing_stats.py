import os.path

import json
import google.cloud.bigquery as bq

from sample_metadata.apis import ProjectApi

_BQ_CLIENT = bq.Client()
_PAPI = ProjectApi()


def get_per_stage_data() -> dict:
    _query = """
    SELECT
        SUM(cost) as cost,
        topic,
        lbl_seqtype.value as sequencing_type,
        lbl_stage.value as stage,
        lbl_tool.value as tool,
        lbl_sample.value as sample
    FROM `billing-admin-290403.billing_aggregate.aggregate`
    CROSS JOIN UNNEST(labels) as lbl_seqtype
    CROSS JOIN UNNEST(labels) as lbl_stage
    CROSS JOIN UNNEST(labels) as lbl_tool
    CROSS JOIN UNNEST(labels) as lbl_sample
    WHERE
        lbl_seqtype.key = 'sequencing_type' AND
        lbl_stage.key = 'stage' AND
        lbl_tool.key = 'tool' AND
        lbl_sample.key = 'sample' AND
        service.id = 'seqr' AND
        service.description NOT IN ('Seqr compute (distributed)', 'Seqr compute (distributed) Credit', 'Seqr Credit')
        AND topic <> 'hail'
    GROUP BY lbl_seqtype.value, lbl_stage.value, lbl_tool.value, lbl_sample.value, topic
    """

    temp_file = 'tmp/per-stage-data.json'

    if not os.path.exists('tmp'):
        os.makedirs('tmp', exist_ok=True)

    if os.path.exists(temp_file):
        print('Using local files')
        with open(temp_file, encoding='utf-8') as f:
            rows = json.load(f)

    else:
        print('Loading results')
        df_bq_result = _BQ_CLIENT.query(_query).result().to_dataframe()
        rows = df_bq_result.to_dict(orient='records')

        with open(temp_file, 'w+', encoding='utf-8') as f:
            json.dump(rows, f)

    by_seqtype_sample_stage_tool = {}
    counter = 0
    for row in rows:
        counter += 1
        components = ['sequencing_type', 'stage', 'tool', 'topic', 'sample']
        keys = [row.get(c) for c in components]
        add_keys_value(by_seqtype_sample_stage_tool, keys, row['cost'])

    calculate_averages(by_seqtype_sample_stage_tool)
    return by_seqtype_sample_stage_tool


def calculate_averages(d):

    for seq_type, seq_body in d.items():
        for stage, stage_body in seq_body.items():
            stage_average = 0
            for tool, tool_body in stage_body.items():
                tool_length = 0
                tool_cost = 0
                for dataset_body in tool_body.values():
                    dataset_cost = sum(dataset_body.values())
                    tool_length += len(dataset_body)
                    tool_cost += dataset_cost
                    dataset_body['_cost'] = dataset_cost
                    dataset_body['_length'] = len(dataset_body)
                    dataset_body['_average'] = dataset_cost / len(dataset_body)

                if tool_length > 0:
                    tool_average = tool_cost / tool_length
                    tool_body['_cost'] = tool_cost
                    tool_body['_average'] = tool_average
                    tool_body['_length'] = tool_length
                    stage_average += tool_average

            stage_body['_average'] = stage_average


def get_elasticsearch_cost() -> dict[str, float]:

    _query = """
    SELECT invoice.month, SUM(cost)
    FROM `billing-admin-290403.billing.gcp_billing_export_v1_01D012_20A6A2_CBD343`
    WHERE sku.description = 'Elastic GCP MP usage'
    GROUP BY invoice.month
    """

    df = _BQ_CLIENT.query(_query).result().to_dataframe()
    d = dict(df.values.tolist())
    return d


def get_storage_cost():
    seqr_projects = _PAPI.get_seqr_projects()
    seqr_project_names = [p['name'] for p in seqr_projects]

    _query = """
    SELECT topic, invoice.month, SUM(cost)
    FROM `billing-admin-290403.billing_aggregate.aggregate`
    WHERE service.description = 'Cloud Storage'
    AND topic IN UNNEST(@topics)
    GROUP BY topic, invoice.month
    """
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter('topics', 'STRING', seqr_project_names),
        ]
    )
    df_bq_result = _BQ_CLIENT.query(_query, job_config=job_config).result()

    print(df_bq_result)


def add_keys_value(d, keys, cost) -> None:
    dd = d
    for k in keys[:-1]:
        if k not in dd:
            dd[k] = {}

        dd = dd[k]

    last_key = keys[-1]
    if last_key in dd:
        dd[last_key] += cost
    else:
        dd[last_key] = cost


def main():
    # es_cost = get_elasticsearch_cost()
    by_seqtype_sample_stage_tool = get_per_stage_data()
    with open('by-seqtype.json', 'w+') as f:
        json.dump(by_seqtype_sample_stage_tool, f, sort_keys=True)

    with open('by-seqtype-no-samples.json', 'w+') as f:
        for sqt, stbody in by_seqtype_sample_stage_tool.items():
            for stg, stgbody in stbody.items():
                for tool, toolbody in stgbody.items():
                    if not isinstance(toolbody, dict):
                        continue
                    stgbody[tool] = {
                        k: toolbody[k] for k in ['_average', '_length', '_cost']
                    }
        json.dump(by_seqtype_sample_stage_tool, f, sort_keys=True, indent=2)


if __name__ == '__main__':
    main()
