DROP PROCEDURE ReportCellChange IF EXISTS;  
DROP PROCEDURE RegisterDevice IF EXISTS;  
DROP PROCEDURE ReportDeviceActivity IF EXISTS;       
DROP PROCEDURE NoteSuspiciousCohort IF EXISTS;       
DROP procedure getSuspectStatus IF EXISTS;
DROP PROCEDURE ShowSimboxActivity__promBL IF EXISTS;
DROP PROCEDURE getSuspectSummary IF EXISTS;
DROP PROCEDURE clearStats IF EXISTS;
DROP PROCEDURE GetDevice IF EXISTS;

DROP view suspicious_devices_view IF EXISTS;
DROP VIEW suspicious_totals_view  IF EXISTS;
DROP VIEW last_3_cells  IF EXISTS;
DROP VIEW last_6_cells  IF EXISTS;

DROP TABLE simbox_parameters  IF EXISTS;
DROP TABLE simbox_stats IF EXISTS;
DROP table cell_table IF EXISTS;
DROP table cell_suspicious_cohorts IF EXISTS;
DROP table cell_suspicious_cohort_members IF EXISTS;
DROP table device_table IF EXISTS;
DROP table device_cell_history IF EXISTS;
DROP table device_incoming_call_history IF EXISTS;
DROP table device_outgoing_call_history IF EXISTS;

DROP FUNCTION add_new_cell  IF EXISTS;
DROP FUNCTION get_last_n_cells  IF EXISTS;


