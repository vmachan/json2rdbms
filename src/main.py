import os
import json
import redshift_connector as rc
import pandas as pd
from jsonwalk import jsonwalk

import config

rs_host=os.getenv('REDSHIFT_HOST')
rs_db=os.getenv('REDSHIFT_DB')
rs_port=os.getenv('REDSHIFT_PORT')
rs_user=os.getenv('REDSHIFT_USER')
rs_pswd=os.getenv('REDSHIFT_PSWD')

rs_conn = rc.connect(host=rs_host, database=rs_db, port=int(rs_port), user=rs_user, password=rs_pswd)
rs_crsr = rs_conn.cursor()
rs_sql = "SELECT jsoncontent_column FROM table "
rs_crsr.execute(rs_sql)

rs_rslt_df = rs_crsr.fetch_dataframe()
for idx, record in rs_rslt_df.iterrows():
    tblr_payload = config.xfrm_payload()
    tblr_payload = jsonwalk(record['jsoncontent_column'], '', tblr_payload)
    for insdict in tblr_payload.ins_stmts:
        if (len(tblr_payload.ins_stmts[insdict]) > 0):
            print("----------------" + str(insdict))
            print(tblr_payload.ins_stmts[insdict])
            print("----------------")

rs_crsr.close()
rs_conn.close()
