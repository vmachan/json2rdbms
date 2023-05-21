import json

# Only output for these tables, used for debugging so we do not have to go thru ALL tables/entities
dbg_tbls =  []

# As of not this JSON XSD file is expected to be in the same folder as this code file. 
# To be enhanced to make it driven off an environment variable. 
src_json_schema_filename = 'root-data.json'
src_json_schema = json.loads(open(src_json_schema_filename, 'r').read())

logfile='/tmp/jsonwalk.log'

class xfrm_payload:
    node_name = ""
    node_jsonpath = ""
    ins_stmts = dict()
    delim_records = dict()
