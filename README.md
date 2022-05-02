# Announcement:

To hugely boost performance and add reliability the engine of the log collector has been rewritten in Rust. Consider downloading the newest
executable to automatically use it. 

If you run python code directly instead of using the executables, install the RustEngine wheel under
the "RustEngineWheels" folder in this repo. To turn off the new engine (in case of issues or for whatever reason), use the following
in your config.yaml:

```
collect:  
  rustEngine: False
```
In my own tests the Rust engine has been at least 10x faster and stable. If you run into any problems, please use the
above setting to revert to the old engine, and consider creating an issue here on Github so I can fix it.

# Office365 audit log collector

Collect/retrieve Office365, Azure and DLP audit logs, optionally filter them, then send them to one or more outputs 
(see full list below).
Onboarding is easy and takes only a few minutes (see 'Onboarding' section). There are Windows and Linux executables.
Configuration is easy with a YAML config file (see the 'ConfigExamples' folder for reference).
If you have any issues or questions, or requests for additional interfaces, feel free to create an issue in this repo.
- The following Audit logs can be extracted:
  - Audit.General
  - Audit.AzureActiveDirectory
  - Audit.Exchange
  - Audit.SharePoint
  - DLP.All
- The following outputs are supported:
  - Azure Analytics Workspace (OMS)
  - Azure Storage Table
  - Azure Storage Blob
  - PRTG Network Monitor
  - ( Azure ) SQL server
  - Graylog (or any other source that accepts a simple socket connection)
  - Fluentd
  - CSV Local file
  - Power BI (indirectly through SQL, CSV, Azure Tables or Azure Blob)

Simply download the executable you need from the Windows or Linux folder and copy a config file from the ConfigExamples
folder that suits your need. Find onboarding instructions and more detailed instructions for using the executables below.

For a full audit trail, schedule to run the collector on a regular basis (preferably at least once every day). Previously
retrieved logs can be remembered to prevent duplicates. Consider using the following parameters in the config file for a robust audit trail:
- skipKnownLogs: True (prevent duplicates)
- hoursToCollect: 24 (or a number larger than the amount of hours between runs, for safety overlap)
- resume: False (don't resume where the last run stopped, have some overlap in case anything was missed for any reason)
See below for a more detailed instruction of the config file.

Lastly, feel free to contribute other outputs if you happen to build any. Also open to any other useful pull requests!
See the following link for more info on the management APIs: https://msdn.microsoft.com/en-us/office-365/office-365-management-activity-api-reference.

## Use cases:

- Ad-lib log retrieval;
- Scheduling regular execution to retrieve the full audit trail
- Output to Graylog/fluentd for full audit trails in SIEM
- Output to PRTG for alerts on audit logs
- Output to (Azure) SQL / CSV for Power BI
- Etc.

## Roadmap:

- Rewrite the collector in Rust. Prototype is finished and runs 5x faster already.

## Latest changes:
- Added native timestamp field to logs for graylog output
- Added fluentd support (thanks @owentl)
- Added Azure Blob and Azure Table outputs
- Added SQL output for Power BI
- Changed file to CSV output
- Added PRTG output
- Added filters
- Added YAML config file
- Added a GUI for Windows
- Added executables for Windows and Linux
- Added Azure Log Analytics Workspace OMS output
- Added parameter to resume from last run time (use to not miss any logs when script hasn't run for a while)
- Added parameter for amount of hours or days to go back and look for content
- Integrated bug fixes from pull requests, thank you!
  - Fix busy loop when connection problem by @furiel
  - New urlencoding for client_secret by @kalimer0x00 
- Fixed bug where script exited prematurely
- Don't start graylog output unnecessarily
- Fixed file output


## Instructions:

### Onboarding (one time only):
- Make sure Auditing is turned on for your tenant!
  - Use these instructions: https://docs.microsoft.com/en-us/microsoft-365/compliance/turn-audit-log-search-on-or-off?view=o365-worldwide
  - If you had to turn it on, it may take a few hours to process
- Create App registration: 
  - Azure AD > 'App registrations' > 'New registration':
    - Choose any name for the registration
    - Choose "Accounts in this organizational directory only (xyz only - Single tenant)"
    - Hit 'register'
    - Save 'Tenant ID' and 'Application (Client) ID' from the overview page of the new registration, you will need it to run the collector
- Create app secret:
  - Azure AD > 'App registrations' > Click your new app registration > 'Certificates and secrets' > 'New client secret':
    - Choose any name and expire date and hit 'add'
      - Actual key is only shown once upon creation, store it somewhere safe. You will need it to run the collector.
- Grant your new app registration 'application' permissions to read the Office API's: 
  - Azure AD > 'App registrations' > Click your new app registration > 'API permissions' > 'Add permissions' > 'Office 365 Management APIs' > 'Application permissions':
    - Check 'ActivityFeed.Read'
    - Check 'ActivityFeed.ReadDlp'
    - Hit 'Add permissions'
- Subscribe to audit log feeds of your choice
  - Set 'autoSubscribe: True' in YAML config file to automate this.
  - OR Use the '--interactive-subscriber' parameter when executing the collector to manually subscribe to the audit API's of your choice
- You can now run the collector and retrieve logs. 


### Running the collector:

You can schedule to run the executable with CRON or Task Scheduler. Alternatively, you can use the "schedule" option in
the YAML config to run the executable once and have it schedule itself (see ConfigExamples/schedule.yaml).

To run the command-line executable use the following syntax:

OfficeAuditLogCollector(.exe) %tenant_id% %client_key% %secret_key% --config %path/to/config.yaml%

To create a config file you can start with the 'fullConfig.yaml' from the ConfigExamples folder. This has all the 
possible options and some explanatory comments. Cross-reference with a config example using the output(s) of your choice, and you
should be set.

### (optional) Creating an Azure Log Analytics Workspace (OMS):

If you are running this script to get audit events in an Azure Analytics Workspace you will need a Workspace ID and a shared key.
- Create a workspace from "Create resource" in Azure (no configuration required);
- Get the ID and key from "Agent management";
- You do not need to prepare any tables or other settings.

### (optional) Creating an Azure Table / Blob account:

If you are running this script to get audit events in an Azure Table and/or Blob you will need a storage account and connection string:
- Create a storage account from "Create resource" in Azure (no special configuration required);
- Get the connection string from 'Access keys'
- You do not need to prepare any tables or blob containers as they are created in the storage account if they do not exist.

### (optional) Creating a PRTG sensor

To run with PRTG you must create a sensor:
- Copy the OfficeAuditLogCollector.exe executable to the "\Custom Sensors\EXE" sub folder of your PRTG installation
- Create a device in PRTG with any host name (e.g. "Office Audit Logs")
- Create a 'EXE/Script Advanced Sensor' on that device and choose the executable you just copied
- Enter parameters, e.g.: "*tenant_id* *client_key* *secret_key* --config *full/path/to/config.yaml*" 
(use full path, because PRTG will execute the script from a different working directory)
- Copy the prtg.config from ConfigExamples and modify at least the channel names and filters for your needs.
- Set the timeout of the script to something generous that suits the amount of logs you will retrieve. 
Probably at least 300 seconds. Run the script manually first to check how long it takes.
- Match the interval of the sensor to the amount of hours of logs to retrieve. If your interval is 1 hour, hoursToCollect
in the config file should also be set to one hour.

### (optional) Using ( Azure ) SQL

If you are running this script to get audit events in an SQL database you will need an ODBC driver and a connection string
- The collector uses PYODBC, which needs an ODBC driver, examples on how to install this:
  - On windows: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15
  - On Linux: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15#ubuntu17
- Connection string might look like this: "Driver={ODBC Driver 17 for SQL Server};Server=tcp:mydatabase.com,1433;Database=mydatabase;Uid=myuser;Pwd=mypassword;Encrypt
=yes;TrustServerCertificate=no;Connection Timeout=30;"
- Use SQL example config and pass --sql-string parameter when running the collector with your connection string



### (optional) Creating a Graylog input

If you are running this script to get audit events in Graylog you will need to create a Graylog input.
- Create a 'raw/plaintext TCP' input;
- Enter the IP and port you want to receive the logs on (you can use these in the script);
- All other settings can be left default.

