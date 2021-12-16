import json
def prep_result_legders(**context):
    source_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    label = source_ledger["label"]
    version = source_ledger["version"]
    base_schema_prefix = "base"
    # check if QACK is used instead    
    if("load_to_qack" in source_ledger["attributes"]):
        base_schema_prefix="qack"    
    schema_name = "{0}{1}t".format(base_schema_prefix,project_code.lower())
    # check if schema is overwritten    
    if("schema_to_overwrite" in source_ledger["attributes"]):
        schema_name=source_ledger["attributes"]["schema_to_overwrite"]
    table_name = "{0}.nrdav2_{1}_{2}".format(schema_name,label,version.replace('-',''))
    if 'versionenabled' in source_ledger["attributes"]:
        if source_ledger["attributes"]["versionenabled"].lower() == 'false':
            table_name = "{0}.nrdav2_{1}".format(schema_name,label)

    result_legders = [{
        "classification": source_ledger["attributes"]["targetclassification"],
        "label": label,
        "version": version,
        "group_count": source_ledger["group_count"],
        "location": "Database",
        "location_details": table_name,
        "attributes":{
            "project": project_code.lower()
        }
    }]

    print(json.dumps(result_legders))
    context['ti'].xcom_push(key='result_legders', value=result_legders)