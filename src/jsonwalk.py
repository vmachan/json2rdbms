import config
import json
import logging

# This is the main logic for walking the JSON content
# This method (jsonwalk) expects the JSON content (json_data), the node for this content (parent_node) and the payload to populate (io_payload)
# It then parses the JSON and when it finds another node to dive into, it calls itself and passes the parameters as needed
# When it finds the lowest level elements i.e. not another node, it then generates an INSERT statement with all the elements for this node
# It adds the INSERT statement to the ins_stmts dictionary in the payload object
#
# Assumptions
# 1. The target table names are same as the JSON node names
# 2. The JSON content is valid i.e. please pre-validate that the JSON is valid before sending it to this method
# Limitation/Enhancements
# 1. As of now, the data type mapping is hard-coded to a few, this could be improved to run off a generic user-defined mapping construct (TBD)

logging.basicConfig(filename=config.logfile, level=logging.WARN)

def jsonwalk(json_data, parent_node, io_payload):
    insert_column_names = ''
    insert_column_vals = ''
    insert_sql_stmt = ''

    logging.info(parent_node)
    curr_node = parent_node.split('.')[-1]
    root_parent_node = 'root' + parent_node

    jdta = json.loads(json_data)

    if (parent_node not in io_payload.ins_stmts):
        io_payload.ins_stmts.update({parent_node: []})
        io_payload.delim_records.update({parent_node: []})

    for key, value in jdta.items():
        try:
            if type(value) == type(dict()):
                io_payload = jsonwalk(json.dumps(value), parent_node + '.' + key, io_payload)
            elif type(value) == type(list()):
                for val in value:
                    if type(val) == type(str()):
                        pass
                    elif type(val) == type(list()):
                        pass
                    else:
                        io_payload = jsonwalk(json.dumps(val), parent_node + '.' + key, io_payload)
            else:
                logging.debug('                 ' + str(key)+'->'+str(value))
                logging.debug('root -> ' + root)

                col_type = config.src_json_schema['definitions'][root_parent_node]['properties'][str(key)]['type']
                col_format = ''
                if ("format" in config.src_json_schema['definitions'][root_parent_node]['properties'][str(key)]):
                    col_format = config.src_json_schema['definitions'][root_parent_node]['properties'][str(key)]['format']
                if (insert_column_names):
                    insert_column_names += (',' + str(key))

                    if (col_type == 'date-time' or col_format == 'date-time'):
                    # if ("time" in str(key) and str(key) != "javatimezone"):
                        insert_column_vals += ',' + 'TO_TIMESTAMP(\'' + str(value) + '\', \'YYYY-MM-DD HH24:MI:SS\')'
                    elif (col_type == 'integer'):
                    # elif (str(value).isnumeric()):
                        insert_column_vals += (',' + str(value) + '')
                    else:
                        insert_column_vals += (',\'' + str(value) + '\'')
                else:
                    insert_column_names += (' ' + str(key))
                    if (col_type == 'date-time' or col_format == 'date-time'):
                    # if ("time" in str(key) and str(key) != "javatimezone"):
                        insert_column_vals += ' ' + 'TO_TIMESTAMP(\'' + str(value) + '\', \'YYYY-MM-DD HH24:MI:SS\')'
                    elif (col_type == 'integer'):
                    # elif (str(value).isnumeric()):
                        insert_column_vals += (' ' + str(value) + '')
                    else:
                        insert_column_vals += (' \'' + str(value) + '\'')
        except KeyError:
            logging.error("Key not found for root_parent_node=[" + root_parent_node + " and str(key)=[" + str(key) + "]")
            pass

    insert_sql_stmt = 'INSERT INTO target_table_schema.' + curr_node + '(' + insert_column_names + ') VALUES (' + insert_column_vals + ')'
    if (curr_node in config.dbg_tbls):
        logging.debug(insert_sql_stmt)
        io_payload.ins_stmts[parent_node].append(insert_sql_stmt)

    return io_payload
