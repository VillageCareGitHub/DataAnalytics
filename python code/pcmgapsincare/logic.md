PCM GAPS IN CARE UPLOAD REASON/LOGIC/FREQUENCY
1. Python script was written to take SQL Server PCM Data and upload data into Redshift to Report GAPS in care with subsequent AWS REDSHIFT views and tables to gather authorizations
2. Script will be run daily using AirFlow to update table
3. Script will then export file to shared drive folder