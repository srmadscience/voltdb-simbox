package simbox;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.util.Date;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class ReportDeviceActivity extends VoltProcedure {

    // @formatter:off

    public static final SQLStmt getDevice = new SQLStmt(
            "SELECT * FROM device_table WHERE device_id = ?;");

    public static final SQLStmt upsertInCall = new SQLStmt(
            "UPSERT INTO device_incoming_call_history "
                    + "( other_number "
                    + "  , cell_id "
                    + "  , end_time "
                    + "  , duration "
                    + "  , status_code "
                    + ", device_id "
                    + ",   start_time ) "
                    + "VALUES "
                    + "(?,?,?,?,?,?,?)");

    public static final SQLStmt upsertOutCall = new SQLStmt(
            "UPSERT INTO device_outgoing_call_history "
                    + "( other_number "
                    + "  , cell_id "
                    + "  , end_time "
                    + "  , duration "
                    + "  , status_code "
                    + ", device_id "
                    + ",   start_time ) "
                    + "VALUES "
                    + "(?,?,?,?,?,?,?)");

    public static final SQLStmt incrementCallHistory = new SQLStmt(
            "UPDATE device_cell_history "
                    + "SET incoming_call_count = incoming_call_count + ? "
                    + "  , outgoing_call_count = outgoing_call_count + ? "
                    + "  , incoming_call_duration = incoming_call_duration + ? "
                    + "  , outgoing_call_duration = outgoing_call_duration + ? "
            + "WHERE device_id = ? AND to_timestamp = MAX_VALID_TIMESTAMP();");
    
    public static final SQLStmt updateDeviceLastSeen = new SQLStmt(
            "UPDATE device_table "
            + "SET last_seen = NOW "
            + "WHERE device_id = ?;");

    public static final SQLStmt getParameter = new SQLStmt(
            "SELECT parameter_value FROM simbox_parameters WHERE parameter_name = ?;");

    public static final SQLStmt getDeviceCellHistory = new SQLStmt(
            "SELECT min(from_timestamp) from_timestamp"
            + "    , sum(incoming_call_count) incoming_call_count"
            + "    , sum(outgoing_call_count) outgoing_call_count "
            + "    , sum(incoming_call_duration) incoming_call_duration"
            + "    , sum(outgoing_call_duration) outgoing_call_duration "
            + "FROM device_cell_history "
            + "WHERE device_id = ? "
            + "AND   from_timestamp >= DATEADD(HOUR, -1 * ?, NOW) ; ");

    public static final SQLStmt getDeviceOutgoingHistorySummary = new SQLStmt(
            "SELECT min(start_time) start_time"
            + "    ,max(end_time) end_time"
            + "    , sum(duration) duration "
            + "    , count(*) how_many "
            + "FROM device_outgoing_call_history "
            + "WHERE device_id = ? "
            + "AND   start_time >= DATEADD(HOUR, -1 * ?, NOW) ; ");

    public static final SQLStmt getDeviceOutgoingHistory = new SQLStmt(
            "SELECT other_number "
            + "    , count(*) how_many "
            + "FROM device_outgoing_call_history "
            + "WHERE device_id = ? "
            + "AND   start_time >= DATEADD(HOUR, -1 * ?, NOW)"
            + "GROUP BY other_number "
            + "ORDER BY count(*) DESC ; ");

    public static final SQLStmt getDeviceIncomingHistorySummary = new SQLStmt(
            "SELECT min(start_time) start_time"
            + "    ,max(end_time) end_time"
            + "    , sum(duration) duration "
            + "    , count(*) how_many "
            + "FROM device_incoming_call_history "
            + "WHERE device_id = ? "
            + "AND   start_time >= DATEADD(HOUR, -1 * ?, NOW) ; ");

    public static final SQLStmt getSuspiciousDeviceIncomingHistorySummary = new SQLStmt(
            "SELECT min(dicm.start_time) start_time"
            + "    ,max(dicm.end_time) end_time"
            + "    , sum(dicm.duration) duration "
            + "    , count(*) how_many "
            + "FROM device_incoming_call_history dicm"
            + "   , suspicious_devices_view v "
            + "WHERE dicm.device_id = ? "
            + "AND   dicm.device_id = v.device_id "
            + "AND   dicm.start_time >= DATEADD(HOUR, -1 * ?, NOW) ; ");


    public static final SQLStmt getSuspiciousDevice = new SQLStmt(
            "SELECT * FROM suspicious_devices_view WHERE device_id = ?;");

 
    public static final SQLStmt flagDevice = new SQLStmt(
            "UPDATE device_table "
            + "SET suspicious_because = ?"
            + "  , suspicious_value = ? "
            + "WHERE device_id = ?;");


    public static final SQLStmt clearDevice = new SQLStmt(
            "UPDATE device_table "
            + "SET suspicious_because = null"
            + "  , suspicious_value = null "
            + "WHERE device_id = ?;");


	// @formatter:on

    public VoltTable[] run(long deviceId, TimestampType startTime, int durationSeconds, String inOrOut,
            long otherNumber, String status) throws VoltAbortException {

        // Note what's changed...
        updateDatabaseTablesForDevice(deviceId, startTime, durationSeconds, inOrOut, otherNumber, status);

        // See if device's behaviour indicates its in a simbox
        seeIfDeviceIsSuspect(deviceId, startTime, durationSeconds, inOrOut, otherNumber, status);

        return voltExecuteSQL(true);
    }

    /**
     * Record the fact that a call has happened.
     * 
     * @param deviceId
     * @param startTime
     * @param durationSeconds
     * @param inOrOut
     * @param otherNumber
     * @param status
     */
    private void updateDatabaseTablesForDevice(long deviceId, TimestampType startTime, int durationSeconds,
            String inOrOut, long otherNumber, String status) {
        // See if we know about this user and transaction...
        voltQueueSQL(getDevice, deviceId);

        VoltTable deviceTable = voltExecuteSQL()[0];

        // Sanity Check: Is this a real user?
        if (!deviceTable.advanceRow()) {
            throw new VoltAbortException("Device " + deviceId + " does not exist");
        }

        long currentCellId = deviceTable.getLong("current_cell_id");

        TimestampType endTime = new TimestampType(
                new Date(startTime.asExactJavaDate().getTime() + (1000 * durationSeconds)));

        if (inOrOut.equalsIgnoreCase("I")) {

            voltQueueSQL(upsertInCall, otherNumber, currentCellId, endTime, durationSeconds, status, deviceId,
                    startTime);
            voltQueueSQL(incrementCallHistory, 1, 0, durationSeconds, 0, deviceId);

        } else {

            voltQueueSQL(upsertOutCall, otherNumber, currentCellId, endTime, durationSeconds, status, deviceId,
                    startTime);
            voltQueueSQL(incrementCallHistory, 0, 1, 0, durationSeconds, deviceId);
        }

        voltQueueSQL(updateDeviceLastSeen, deviceId);

        voltExecuteSQL();
    }

    /**
     * See if given device might be in a simbox by looking at its behaviour.
     * 
     * @param deviceId
     * @param startTime
     * @param durationSeconds
     * @param inOrOut
     * @param otherNumber
     * @param status
     */
    @SuppressWarnings("unused")
    private void seeIfDeviceIsSuspect(long deviceId, TimestampType startTime, int durationSeconds, String inOrOut,
            long otherNumber, String status) {

        // These parameters affect the decision making logic.
        voltQueueSQL(getParameter, "OUTGOING_CALL_ONLY_COUNT");
        voltQueueSQL(getParameter, "IMCOMING_CALL_ONLY_COUNT");
        voltQueueSQL(getParameter, "OUTGOING_INCOMING_RATIO");
        voltQueueSQL(getParameter, "NOT_NEW_ANY_MORE_DAYS");
        voltQueueSQL(getParameter, "BUSYNESS_PERCENTAGE");
        voltQueueSQL(getParameter, "HOURS_BACK_TO_CHECK");
        voltQueueSQL(getParameter, "TOP_N");
        voltQueueSQL(getParameter, "TOP_BOTTOM_N_RATIO");

        voltQueueSQL(getDevice, EXPECT_ONE_ROW, deviceId);
        voltQueueSQL(getSuspiciousDevice, deviceId);

        VoltTable[] firstResults = voltExecuteSQL();

        final long outgoingCallThreshold = getParameter(2, firstResults[0]);
        final long incomingCallThreshold = getParameter(2, firstResults[1]);
        final long outgoingIncoming = getParameter(10, firstResults[2]);
        final long notNewAnyMoreDays = getParameter(10, firstResults[3]);
        final long busynessPercentage = getParameter(30, firstResults[4]);
        final long hoursBackToCheck = getParameter(3, firstResults[5]);
        final long topN = getParameter(5, firstResults[6]);
        final long topBottomNRatio = getParameter(10, firstResults[7]);

        VoltTable device = firstResults[8];
        device.advanceRow();

        VoltTable suspiciousDevice = firstResults[9];
        boolean thisDeviceIsSuspicious = false;

        if (suspiciousDevice.advanceRow()) {
            thisDeviceIsSuspicious = true;
        }

        final TimestampType deviceFirstSeen = device.getTimestampAsTimestamp("first_seen");
        String suspiciousBecause = device.getString("suspicious_because");

        if (suspiciousBecause == null) {
            suspiciousBecause = new String("");
        }

        final Date deviceIsYoungEnoughToWorryAbout = new Date(
                this.getTransactionTime().getTime() - (notNewAnyMoreDays * 24 * 60 * 60 * 1000));

        if (deviceIsYoungEnoughToWorryAbout.before(deviceFirstSeen.asApproximateJavaDate())) {

            voltQueueSQL(getDeviceCellHistory, deviceId, hoursBackToCheck);
            voltQueueSQL(getDeviceOutgoingHistorySummary, deviceId, hoursBackToCheck);
            voltQueueSQL(getDeviceIncomingHistorySummary, deviceId, hoursBackToCheck);
            voltQueueSQL(getSuspiciousDeviceIncomingHistorySummary, deviceId, hoursBackToCheck);
            voltQueueSQL(getDeviceOutgoingHistory, deviceId, hoursBackToCheck);

            VoltTable[] secondResults = voltExecuteSQL();

            long incomingCallCount = 0;
            long incomingCallDuration;
            long outgoingCallCount = 0;
            long outgoingCallDuration;

            VoltTable cellHistory = secondResults[0];
            if (cellHistory.advanceRow()) {

                incomingCallCount = cellHistory.getLong("incoming_call_count");
                incomingCallDuration = cellHistory.getLong("incoming_call_duration");
                outgoingCallCount = cellHistory.getLong("outgoing_call_count");
                outgoingCallDuration = cellHistory.getLong("outgoing_call_duration");

            }

            long actualBusyOutCallPct = getActualBusyOutCallPct(outgoingCallThreshold, secondResults[1]);

            long actualBusyInCallPct = getActualBusyInCallPct(incomingCallThreshold, secondResults[2]);

            long actualBusyInCallSuspicuousPct = getActualBusyInCallSuspiciousPct(secondResults[3]);

            long outCallTopBottomNRatio = getTopNRatio(secondResults[4], (int) topN);

            // Decide what kind of device this is...
            if (thisDeviceIsSuspicious // Known suspicious number
                    && actualBusyInCallPct >= 1 // We have incoming calls..
                    && actualBusyInCallSuspicuousPct == actualBusyInCallPct) // All of them are from bad numbers
            {

                voltQueueSQL(flagDevice, "all_incoming_calls_from_known_bad_numbers", actualBusyInCallSuspicuousPct,
                        deviceId);

            } else if (thisDeviceIsSuspicious && // Known suspicious number
                    actualBusyInCallSuspicuousPct > 1) { // At least one call from a bad number

                voltQueueSQL(flagDevice, "some_incoming_calls_from_known_bad_numbers", actualBusyInCallSuspicuousPct,
                        deviceId);

            } else if (thisDeviceIsSuspicious // Known suspicious number
                    && incomingCallCount == 0 // no incoming calls
                    && outgoingCallCount > 0 // some outgoing calls
            ) {
                voltQueueSQL(flagDevice, "suspicious_device_has_no_incoming_calls", actualBusyOutCallPct, deviceId);

            } else if (thisDeviceIsSuspicious) { // Device is part of a group that have all moved together >= 6 times

                voltQueueSQL(flagDevice, "suspiciously_moving_device", actualBusyOutCallPct, deviceId);

            } else if ((actualBusyInCallPct + actualBusyOutCallPct) >= busynessPercentage // We're very busy
                    && (outgoingIncoming * incomingCallCount) < outgoingCallCount) { // Lots of calls out

                voltQueueSQL(flagDevice, "total_incoming_outgoing_ratio_bad",
                        actualBusyInCallPct + actualBusyOutCallPct, deviceId);

            } else if ((actualBusyInCallPct + actualBusyOutCallPct) >= busynessPercentage // We're very busy
                    && outCallTopBottomNRatio < topBottomNRatio) { // Lots of calls out

                voltQueueSQL(flagDevice, "topn_incoming_outgoing_ratio_bad", outCallTopBottomNRatio, deviceId);

            } else {
                voltQueueSQL(clearDevice, deviceId);
            }

        }

    }

    /**
     * Determine what % of time is spent making calls
     * @param outgoingCallThreshold min number of calls before we care
     * @param outCallHistory Call history
     * @return 0 or pct spent making calls
     */
    private long getActualBusyOutCallPct(final long outgoingCallThreshold, VoltTable outCallHistory) {
        long actualBusyOutCallPct = 0;

        if (outCallHistory.advanceRow()) {

            final TimestampType firstSeen = outCallHistory.getTimestampAsTimestamp("start_time");
            final TimestampType lastSeen = outCallHistory.getTimestampAsTimestamp("end_time");
            long outgoingCallDurationRecent = outCallHistory.getLong("duration");
            long howMany = outCallHistory.getLong("how_many");

            if (howMany > outgoingCallThreshold) {
                // calculate time period covered in seconds
                final long timePeriodSeconds = (lastSeen.asApproximateJavaDate().getTime()
                        - firstSeen.asApproximateJavaDate().getTime()) / 1000;

                if (timePeriodSeconds > 0) {
                actualBusyOutCallPct = (100 * outgoingCallDurationRecent) / timePeriodSeconds;
                }
            }
        }
        return actualBusyOutCallPct;
    }

    /**
     * Determine what % of time is spent receiving calls
     * @param outgoingCallThreshold min number of calls before we care
     * @param outCallHistory Call history
     * @return 0 or pct spent receiving calls
     */ 
    private long getActualBusyInCallPct(final long outgoingCallOnlyCount, VoltTable inCallHistory) {
        long actualBusyInCallPct = 0;

        if (inCallHistory.advanceRow()) {

            final TimestampType firstSeen = inCallHistory.getTimestampAsTimestamp("start_time");
            final TimestampType lastSeen = inCallHistory.getTimestampAsTimestamp("end_time");
            long incomingCallDurationRecent = inCallHistory.getLong("duration");
            long howMany = inCallHistory.getLong("how_many");

            if (howMany > outgoingCallOnlyCount) {
                // calculate time period covered in seconds
                final long timePeriodSeconds = (lastSeen.asApproximateJavaDate().getTime()
                        - firstSeen.asApproximateJavaDate().getTime()) / 1000;
                if (timePeriodSeconds > 0) {
                actualBusyInCallPct = (100 * incomingCallDurationRecent) / timePeriodSeconds;
                }
            }
        }
        return actualBusyInCallPct;
    }

    /**
     * Determine what % of time is spent receiving calls from numbers we think are suspicious
     * @param outCallHistory Call history
     * @return pct spent receiving calls
     */ 
     private long getActualBusyInCallSuspiciousPct(VoltTable suspiciousInCallHistorySummary) {

        long actualBusyInCallSuspicuousPct = 0;

        if (suspiciousInCallHistorySummary.advanceRow()) {

            final TimestampType firstSeen = suspiciousInCallHistorySummary.getTimestampAsTimestamp("start_time");
            final TimestampType lastSeen = suspiciousInCallHistorySummary.getTimestampAsTimestamp("end_time");
            long incomingCallDurationRecent = suspiciousInCallHistorySummary.getLong("duration");
            long howMany = suspiciousInCallHistorySummary.getLong("how_many");

            if (howMany > 0) {
                // calculate time period covered in seconds
                final long timePeriodSeconds = (lastSeen.asApproximateJavaDate().getTime()
                        - firstSeen.asApproximateJavaDate().getTime()) / 1000;

                if (timePeriodSeconds > 0) {
                    actualBusyInCallSuspicuousPct = (100 * incomingCallDurationRecent) / timePeriodSeconds;

                }

            }
        }

        return actualBusyInCallSuspicuousPct;
    }

    /**
     * Get ratio between total number of calls for top 'n' busiest numbers and 
     * bottom 'n' busiest numbers
     * 
     * @param suspiciousInCallHistory
     * @param n - how many to compare 
     * @return Integer.MAX_VALUE if less than n * 2 calls, otherwise ratio top n : bottom n
     */
    private int getTopNRatio(VoltTable suspiciousInCallHistory, int n) {

        int topNCalls = 0;
        int bottomNCalls = 0;

        if (suspiciousInCallHistory.getRowCount() >= (n * 2)) {

            while (suspiciousInCallHistory.advanceRow()) {

                if (suspiciousInCallHistory.getActiveRowIndex() < n) {
                    topNCalls += suspiciousInCallHistory.getLong("how_many");
                } else if (suspiciousInCallHistory.getActiveRowIndex() > suspiciousInCallHistory.getRowCount() - n) {
                    bottomNCalls += suspiciousInCallHistory.getLong("how_many");
                }

            }

        } else {
            return Integer.MAX_VALUE;
        }

        return topNCalls / bottomNCalls;
    }

    /**
     * Get a parameter or its default value
     * 
     * @param value          - default value
     * @param parameterTable
     * @return a parameter or its default value
     */
    protected long getParameter(long value, VoltTable parameterTable) {
        if (parameterTable.advanceRow()) {
            value = parameterTable.getLong("parameter_value");
        }
        return value;
    }

}
