{% seo %}

# Office365 audit log collector

Collect/retrieve Office365, Azure and DLP audit logs, optionally filter them, then send them to one or more outputs such as file, PRTG, Azure Log Analytics or Graylog.
Onboarding is easy and takes only a few minutes (steps described below). There are Windows and Linux executables, and an optional GUI for Windows only.
Easy configuration with a YAML config file (see the 'ConfigExamples' folder for reference).
If you have any issues or questions, feel free to create an issue in this repo.
- The following Audit logs can be extracted:
  - Audit.General
  - Audit.AzureActiveDirectory
  - Audit.Exchange
  - Audit.SharePoint
  - DLP.All
- The following outputs are supported:
  - Azure Analytics Workspace (OMS)
  - PRTG Network Monitor
  - Graylog (or any other source that accepts a simple socket connection)
  - Local file

Simply download the executable you need from the Windows or Linux folder and copy a config file from the ConfigExamples folder that suits your need:
- Windows:
  - GUI-OfficeAuditLogCollector.exe
    - GUI for collecting audit logs and subscribing to audit log feeds
  - OfficeAuditLogCollector.exe
    - Command line tool for collecting audit logs and (automatically) subscribing to audit log feeds
- Linux:
  - OfficeAuditLogCollector
    - Command line tool for collecting audit logs and (automatically) subscribing to audit log feeds

Find onboarding instructions and more detailed instructions for using the executables below.

For a full audit trail, schedule to run the collector on a regular basis (preferably at least once every day). Previously
retrieved logs can be remembered to prevent duplicates. Consider using the following parameters in the config file for a robust audit trail:
- skipKnownLogs: True (prevent duplicates)
- hoursToCollect: 24 (the maximum, or a number larger than the amount of hours between runs, for safety overlap)
- resume: False (don't resume where the last run stopped, have some overlap in case anything was missed for any reason)
See below for a more detailed instruction of the config file.

Lastly, feel free to contribute other outputs if you happen to build any. Also open to any other useful pull requests!
See the following link for more info on the management APIs: https://msdn.microsoft.com/en-us/office-365/office-365-management-activity-api-reference.

## Roadmap:

- Automate onboarding as much as possible to make it easier to use
- Make a container that runs this script
- Create a tutorial for automatic onboarding + docker container for the easiest way to run this

## Latest changes:
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

## Use cases:

- Ad-lib log retrieval;
- Scheduling regular execution to retrieve the full audit trail.
- Output to PRTG for alerts on audit logs

## Features:

- Subscribe to the audit logs of your choice through the --interactive-subscriber switch, or automatically when collecting logs;
- Collect General, Exchange, Sharepoint, Azure active directory and/or DLP audit logs through the collector script;
- Output to file, PRTG, Azure Log Analytics or to a Graylog input (i.e. send the logs over a network socket).

## Requirements:
- Office365 tenant;
- Azure app registration created for this script (see onboarding instructions)
- Secret key (created in the new Azure app registration, see instructions);
- App permissions to access the APIs for the new Azure application (see instructions);
- Subscription to the APIs of your choice (use autoSubscribe option in the config file to automate this).

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

Running from the GUI should be self-explanatory. It can run once or on a schedule. Usually you will want to use the 
command-line executable with a config file, and schedule it for periodic execution (e.g. through CRON, windows task
scheduler, or a PRTG sensor).

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

### (optional) Creating a Graylog input

If you are running this script to get audit events in Graylog you will need to create a Graylog input.
- Create a 'raw/plaintext TCP' input;
- Enter the IP and port you want to receive the logs on (you can use these in the script);
- All other settings can be left default.

