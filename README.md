# Outreach ETL Tool

This ETL script will:

- Pulls raw data from [Outreach](https://api.outreach.io/api/v2/docs)
- Extracts the following resources:
  - [Prospects](https://api.outreach.io/api/v2/prospects)
  - [Sequences](https://api.outreach.io/api/v2/sequences)
  - [Mailings](https://api.outreach.io/api/v2/mailings)
  - [Accounts](https://api.outreach.io/api/v2/accounts)
  - [Opportunities](https://api.outreach.io/api/v2/opportunities)
- Uploads to Google BigQuery
- Incrementally pulls data

**Available Outreach Scopes**

Available scopes: profile, email, create_prospects, read_prospects, update_prospects, read_sequences, update_sequences, read_tags, read_accounts, create_accounts, read_activities, read_mailings, read_mappings, read_plugins, read_users, create_calls, read_calls, read_call_purposes, read_call_dispositions, accounts.all, accounts.read, accounts.write, accounts.delete, callDispositions.all, callDispositions.read, callDispositions.write, callDispositions.delete, callPurposes.all, callPurposes.read, callPurposes.write, callPurposes.delete, calls.all, calls.read, calls.write, calls.delete, events.all, events.read, events.write, events.delete, mailings.all, mailings.read, mailings.write, mailings.delete, mailboxes.all, mailboxes.read, mailboxes.write, mailboxes.delete, personas.all, personas.read, personas.write, personas.delete, prospects.all, prospects.read, prospects.write, prospects.delete, sequenceStates.all, sequenceStates.read, sequenceStates.write, sequenceStates.delete, sequenceSteps.all, sequenceSteps.read, sequenceSteps.write, sequenceSteps.delete, sequences.all, sequences.read, sequences.write, sequences.delete, stages.all, stages.read, stages.write, stages.delete, taskPriorities.all, taskPriorities.read, taskPriorities.write, taskPriorities.delete, users.all, users.read, users.write, users.delete, tasks.all, tasks.read, tasks.write, tasks.delete, snippets.all, snippets.read, snippets.write, snippets.delete, templates.all, templates.read, templates.write, templates.delete, rulesets.all, rulesets.read, rulesets.write, rulesets.delete, opportunities.all, opportunities.read, opportunities.write, opportunities.delete, opportunityStages.all, opportunityStages.read, opportunityStages.write, opportunityStages.delete, sequenceTemplates.all, sequenceTemplates.read, sequenceTemplates.write, sequenceTemplates.delete, customValidations.all, customValidations.read, customValidations.write, customValidations.delete, webhooks.all, webhooks.read, webhooks.write, webhooks.delete, teams.all, teams.read, teams.write, teams.delete, mailboxContacts.all, mailboxContacts.read, mailboxContacts.write, mailboxContacts.delete, meetingTypes.all, meetingTypes.read, meetingTypes.write, meetingTypes.delete, experiments.all, experiments.read, experiments.write, experiments.delete, phoneNumbers.all, phoneNumbers.read, phoneNumbers.write, phoneNumbers.delete, meetingFields.all, meetingFields.read, meetingFields.write, meetingFields.delete, customDuties.all, customDuties.read, customDuties.write, customDuties.delete, duties.all, duties.read, duties.write, duties.delete, favorites.all, favorites.read, favorites.write, favorites.delete, emailAddresses.all, emailAddresses.read, emailAddresses.write, emailAddresses.delete

## Quickstart

### Create a Config file

```
{
  "client_id": "client_id",
  "client_secret": "client_secret",
  "refresh_token": "refresh_token",
  "scope": "prospects.read sequences.read accounts.read opportunities.read mailings.read",
  "redirect_uri": "redirect_uri",
  "replication_type": "full",
  "start_date": "2019-01-01",
  "company": "company_name",
  "email": "email_to_send_log_report",
  "project": "gbq_project",
  "dataset": "gbq_project",
  "table": "gbq_table"
}
```

The `client_id` and `client_secret` keys are your OAuth Salesforce App secrets. The `refresh_token` is a secret created during the OAuth flow. For more info on the Salesforce OAuth flow, visit the [Outreach documentation](https://api.outreach.io/api/v2/docs#authentication).

The `replication_type` should be set to either `full` or `previous_day`. If `full` is chosen, then replication will start from the provided `start_date`. `previous_day` should be chosen if the script will be run via cron job.

### Sync Data

```
> python outreach_etl.py --config sample_creds.json
```
