# AF_export 
AppsFlyer Export Data Python Script

DIRECTIONS FOR RUNNNING SCRIPT
==============================
Requirements: 
Python 3.x
Python Libraries:
Pandas


usage: value.py [-h] [--format FORMAT] [--output OUTPUT] csvfile col

Add columns to AppsFlyer csv export file by processing the json or urlparams
in any column.

positional arguments:
  csvfile          AppsFlyer csv export
  col              Column Name to parse (json or url with params)

optional arguments:
  -h, --help       show this help message and exit
  --format FORMAT  'json' or 'urlparams'. The default is 'urlparams'
  --output OUTPUT  Output file name. If not provided, the output file will be
                   named output.csv

# Examples
Run the following on the command line 
- ./value.py MyCompanyInstalls.csv "Original URL"
- ./value.py MyCompanyInAppEvents.csv "Event Value" --format=json
- ./value.py MyCompanyInstalls.csv "Original URL" --format=urlparams
--output=MyCompanyOutput.csv

