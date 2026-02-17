## run a gear via SDK
##

import flywheel
import pandas as pd
import os

api_key = os.getenv('FW_API_KEY')
fw = flywheel.Client(api_key)

# ========================  Set up =========================================
local_data_dir = 'az'
fw_group_label = 'astrazeneca'
fw_destination_proj = f'{fw_group_label}/az_imaging_delivery' # destination project for copied data
sub_df = pd.read_csv(f'{local_data_dir}/cbtn_selected_fw_sessions.csv') # output of find_fw_data.py
sub_df = sub_df.drop(columns=['Session']).drop_duplicates().reset_index(drop=True)

# ========================= Main processes ========================================
## Initialize gear
# define the gear name (needs to match that in Flywheel config file)
gear_name = 'soft-copy'
gear2run = fw.lookup(f'gears/{gear_name}')

# iterate through projects and queue job for each
proj_list = sub_df['Project'].drop_duplicates().tolist()

for project_label in proj_list:
    # load the Flywheel container for this source project
    source_project = fw.projects.find_first(f'label={project_label}')

    # select subjects in this project
    this_sub_df = sub_df[sub_df['Project']==project_label]
    sub_list = this_sub_df['CBTN Subject ID'].tolist()
    sub_id_string = ",".join(sub_list)

    # define gear configuration
    config = {'duplicate-strategy': 'skip',
                'subjects-to-include': sub_id_string,
                'intermediate-group':fw_group_label,
                'target-project':fw_destination_proj,
                }

    # initialize destination project if doesn't already exist
    try:
        dest_project = fw.lookup(fw_destination_proj)
    except:
        dest_grp = fw_destination_proj.split('/')[0]
        dest_proj = fw_destination_proj.split('/')[1]
        proj_id = fw.add_project({'label':dest_proj,
                                 'group':dest_grp})

    # Run the job
    job_id = gear2run.run(config=config, destination=source_project)

    print(f'Queued soft-copy for project {project_label}, job ID: {job_id}')
