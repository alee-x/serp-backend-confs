import json

def is_post_processing_required(**context):
    ledger = context['dag_run'].conf['ledger']
    schema = json.loads(ledger["attributes"]["schema"])

    enc_col_list = []
    if schema["return_info"]["schema_cols_to_enc"]:
        enc_col_list = [d['enced_col_name'].lower() for d in schema["return_info"]["schema_cols_to_enc"] if d['enc_location'].lower() == 'db'] 

    if(enc_col_list):
        return "run_xref_load"
    else:
        print("post process is skipped as no columns to encrypt")
        return "run_base_load_without_postprocess"