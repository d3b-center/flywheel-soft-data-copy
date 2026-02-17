
import pandas as pd
import os
import flywheel
import psycopg2

DB_URL = os.getenv('D3B_WAREHOUSE_URL')
DB_USER = os.getenv('D3b_DB_SVC_USER')
DB_PASSWORD = os.getenv('D3b_DB_SVC_PASSWORD')

def find_fw_data(fw, source, sub_df, level='session'):

    if source == 'd3b_warehouse':
        # pull project/subject info from D3b warehouse table that captures all Flywheel CBTN MRI data
        conn = psycopg2.connect(
                host=DB_URL,
                database="postgres",
                user=DB_USER,
                password=DB_PASSWORD,
                port="5432" # default is 5432
            )
        cur = conn.cursor()
        if level == 'subject':
            cur.execute("SELECT DISTINCT project_label,subject_label FROM src_imaging_platforms.flywheel_cbtn_mri_export;")
        else:
            cur.execute("SELECT DISTINCT project_label,subject_label,session_label FROM src_imaging_platforms.flywheel_cbtn_mri_export;")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if level == 'subject':
            cbtn_fw_df = pd.DataFrame(rows, columns=['Project', 'CBTN Subject ID'])
        else:
            cbtn_fw_df = pd.DataFrame(rows, columns=['Project', 'CBTN Subject ID', 'Session'])
        fw_data_df = sub_df.merge(cbtn_fw_df, on='CBTN Subject ID', how='inner')

    elif source == 'flywheel':
        # pull project/subject info directly from Flywheel
        rows = []
        n_subjs = len(sub_df)
        for ind,row in sub_df.iterrows():
            sub_id = row['CBTN Subject ID']
            session_id = row['Session']
            print(f'PROCESSING SUBJECT {ind+1}/{n_subjs}: {sub_id}')
            # query = f'group.label = d3b AND '\
            #         f'project.label CONTAINS _v2 AND '\
            #         f'subject.label = {sub_id} '
            # matching_projs = fw.search({'structured_query': query, 'return_type': 'project'}, size=10000)
            # for result in matching_projs:
            #     if [result.project.label, sub_id ] not in rows:
            #         rows.append([result.project.label, sub_id ])
            # this method is faster than the query method above:
            sub_projs = fw.subjects.find(f'label={sub_id}') # returns list of projects subject is in
            for sub_cntr in sub_projs:
                # narrow to 'D3b' projects
                if (sub_cntr.parents.group == 'd3b'):
                    proj_id = sub_cntr.project
                    proj_cntr = fw.get_project(proj_id)
                    # narrow to "_v2" projects
                    if ('_v2' in proj_cntr.label):
                        if [proj_cntr.label, sub_id ] not in rows:
                            rows.append([proj_cntr.label, sub_id, session_id])

        fw_data_df = pd.DataFrame(rows, columns=['Project', 'CBTN Subject ID', 'Session'])

    return fw_data_df