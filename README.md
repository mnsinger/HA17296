# HA17296

### Summary

This script takes data from a TeleTracking file regarding Pending Discharges and sends notifications to the physician attending the patients scheduled for discharge on that day.

### Process

- Opens up a TeleTracking csv file on a network drive that gets recreated daily and takes in info. File contains one line per patient pending discharge.
- Takes physician name and gets email address for the physician. Generates an html email with all patients for each physician.
- Sends out one email per physician with all their patients pending discharge for the day.
