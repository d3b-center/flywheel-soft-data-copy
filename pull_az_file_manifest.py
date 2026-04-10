# Pull AZ manifest from Flywheel

import flywheel
import os
from datetime import date

todays_date = date.today().isoformat() # formatted as YYYY-MM-DD

fw = flywheel.Client(os.getenv('FW_API_KEY'))

project = fw.get_project('69038dc3391cc4e98e301a94') # astrazeneca/az_imaging_delivery
view = fw.get_view('69a708c56c7c0e860041f523') # "AZ manifest"

print(f'Pulling DataView for project: {project.label}')
df = fw.read_view_dataframe(view, project.id)

# get age in days from session.label (split on string 'd_)
df['ScanDate'] = df['Timepoint'].str.split('d_').str[0]

df['SourceName'] = 'CHP'

# construct file path on s3
df['s3_path'] = 's3://opendata-chop-study-us-east-1-prd-sd-bhjxbdqk/astrazeneca/imaging/az_imaging_delivery/' + df['SubjectID'] + "/"+ df['Timepoint'] +"/" +df['acquisition.label'] +"/" + df['FileName']

# re-order columns to match AZ template
df = df[['FileName','FileFormat','ImageModality','ScanDate','SourceName','SubjectID','Timepoint','s3_path']]

df.to_csv(f'AZ_manifest_{todays_date}.csv', index=False)
