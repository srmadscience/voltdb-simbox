UPSERT INTO simbox_stats VALUES ('simboxstatus_not_suspected',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_some_incoming_calls_from_known_bad_numbers',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_suspicious_device_has_no_incoming_calls',0);
UPSERT INTO simbox_stats VALUES ('simboxstatus_suspiciously_moving_device',0);

UPSERT INTO simbox_stats VALUES ('suspicious_because_some_incoming_calls_from_known_bad_numbers',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_suspicious_device_has_no_incoming_calls',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_suspiciously_moving_device',0);
UPSERT INTO simbox_stats VALUES ('suspicious_because_all_incoming_calls_from_known_bad_numbers',0);
 UPSERT INTO simbox_stats VALUES ('suspicious_because_total_incoming_outgoing_ratio_bad',0);
 UPSERT INTO simbox_stats VALUES ('suspicious_because_topn_incoming_outgoing_ratio_bad',0);

