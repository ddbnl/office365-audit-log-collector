# Office365 API audit log collector

Collect Office365 and Azure audit logs through their respective APIs. No prior knowledge of APIs is required, 
onboarding and script usage is described below. There is a GUI for Windows. Currently supports the following outputs:
- Azure Analytics Workspace (OMS)
- Graylog (or any other source that accepts a simple socket connection)
- File

Simply download the executable(s) you need from the Windows or Linux folder.:
- Windows:
  - GUI - Office Audit Log Collector.exe
    - GUI for collecting audit logs AND subscribing to audit log feeds (see onboarding instructions below)
  - Office Audit Log Collector.exe
    - Command line tool for collecting audit logs (see syntax below)
  - Office Audit Log Subscriber.exe
    - Command line tool for subscribing to audit logs feeds (see onboarding instructions below)
- Linux:
  - OfficeAuditLogCollector
    - Command line tool for collecting audit logs (see syntax below)
  - OfficeAuditLogSubscriber
    - Command line tool for subscribing to audit logs (see onboarding instructions below)

For a full audit trail schedule to run the script on a regular basis (preferably at least once every day). The last
run time is recorded automatically, so that when the script runs again it starts to retrieve audit logs from when it last ran.
Feel free to contribute other outputs if you happen to build any.
See the following link for more info on the management APIs: https://msdn.microsoft.com/en-us/office-365/office-365-management-activity-api-reference.

## Roadmap:

- Automate onboarding as much as possible to make it easier to use
- Make a container that runs this script
- Create a tutorial for automatic onboarding + docker container for the easiest way to run this

## Latest changes:
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

## Use cases:

- Ad-lib log retrieval;
- Scheduling regular execution to retrieve the full audit trail.

## Features:

- Subscribe to the audit logs of your choice through the subscription script;
- Collect General, Exchange, Sharepoint, Azure active directory and/or DLP audit logs through the collector script;
- Output to file or to a Graylog input (i.e. send the logs over a network socket)

## Requirements:
- Office365 tenant;
- Azure app registration created for this script (see instructions)
- AzureAD tenant ID;
- Client key of the new Azure app registration;
- Secret key (created in the new Azure app registration, see instructions);
- App permissions to access the APIs for the new Azure application (see instructions);
- Subscription to the APIs of your choice (General/Sharepoint/Exchange/AzureAD/DLP, run AuditLogSubscription script and follow the instructions).

## Instructions:

### Onboarding:
- Create an app registration: 
  - Create the app registration itself under Azure AD (own tenant only works fine for single tenant)
  - Create app secret (only shown once upon creation, store it somewhere safe)
  - Grant your new app permissions to read the Office API's: 
      - Graph: AuditLog.Read.All
      - Office 365 Management APIs: ActivityFeed.Read
      - Office 365 Management APIs: ActivityFeed.ReadDlp
- Make sure Auditing is turned on for your tenant!
  - https://docs.microsoft.com/en-us/microsoft-365/compliance/turn-audit-log-search-on-or-off?view=o365-worldwide
  - If you had to turn it on, it may take a few hours to process
- Use the 'AuditLogSubscriber' script to subscribe to the audit API's of your choice
  - You will need tenant id, client key and secret key for this
  - Simply follow the instructions
- You can now run the script and retrieve logs. 


### (optional) Creating an Azure Log Analytics Workspace (OMS):

If you are running this script to get audit events in an Azure Analytics Workspace you will a Workspace ID and a shared key.
Create a workspace from "Create resource" in Azure (no configuration required). Then get the ID and key from "Agent management".
You do not need to prepare any tables or other settings.


### (optional) Creating a Graylog input

If you are running this script to get audit events in Graylog you will need to create a Graylog input. If not, just skip this.

- Create a 'raw/plaintext TCP' input;
- Enter the IP and port you want to receive the logs on (you can use these in the script);
- All other settings can be left default.


### Running the script:

- Retrieve all logs and send to a network socket / Graylog server:
`python3 AuditLogCollector.py 'tenant_id' 'client_key' 'secret_key' --exchange --dlp --azure_ad --general --sharepoint -p 'random_publisher_id' -g -gA 10.10.10.1 -gP 6000`

#### Script options:
```
usage: AuditLogCollector.py [-h] [--general] [--exchange] [--azure_ad]
                            [--sharepoint] [--dlp] [-p publisher_id]
                            [-l log_path] [-f] [-fP file_output_path] [-g]
                            [-gA graylog_address] [-gP graylog_port]
                            tenant_id client_key secret_key`
                            
positional arguments:
  tenant_id             Tenant ID of Azure AD
  client_key            Client key of Azure application
  secret_key            Secret key generated by Azure application`

optional arguments:
  -h, --help            show this help message and exit
  --general             Retrieve General content
  --exchange            Retrieve Exchange content
  --azure_ad            Retrieve Azure AD content
  --sharepoint          Retrieve SharePoint content
  --dlp                 Retrieve DLP content
  -r                    Resume looking for content from last run time for each content type (takes precedence over -tH and -tD)
  -tH                   Number of hours to to go back and look for content
  -tD                   Number of days to to go back and look for content
  -p publisher_id       Publisher GUID to avoid API throttling
  -l log_path           Path of log file
  -f                    Output to file.
  -fP file_output_path  Path of directory of output files
  -a                    Output to Azure Log Analytics workspace
  -aC                   ID of log analytics workspace.
  -aS                   Shared key of log analytics workspace.
  -g                    Output to graylog.
  -gA graylog_address   Address of graylog server.
  -gP graylog_port      Port of graylog server.
  -d                    Enable debug logging (large log files and lower performance)
```