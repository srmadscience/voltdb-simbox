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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Procedure to find groups of devices that all move cell at the same time
 * repeatedly, which is suspicious.
 * 
 *
 */
public class NoteSuspiciousCohort extends VoltProcedure {

    // @formatter:off


    public static final SQLStmt getSuspiciousDevices = new SQLStmt(
            "SELECT device_id, current_cell_id "
            + "FROM device_table "
            + "WHERE cell_history_as_string_last6  = ? "
            + "ORDER BY device_id;");

    public static final SQLStmt createNewCohort = new SQLStmt(
            "UPSERT INTO cell_suspicious_cohorts "
            + "VALUES "
            + "(?,NOW);");

    public static final SQLStmt createNewCohortMember = new SQLStmt(
            "UPSERT INTO cell_suspicious_cohort_members "
            + "VALUES "
            + "(?,NOW,?);");
    
  	// @formatter:on

    /**
     * @param cellHistoryBusiest a list of cell movement signatures which lots of
     *                           devices share
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(String[] cellHistoryBusiest) throws VoltAbortException {

        for (int i = 0; i < cellHistoryBusiest.length; i++) {
            // Get Devices...
            voltQueueSQL(getSuspiciousDevices, cellHistoryBusiest[i]);

            VoltTable suspiciousDevices = voltExecuteSQL()[0];

            // Record existence of cohort and members...
            long cellId = Long.MIN_VALUE;
            while (suspiciousDevices.advanceRow()) {

                if (cellId == Long.MIN_VALUE) {
                    cellId = suspiciousDevices.getLong("current_cell_id");
                    voltQueueSQL(createNewCohort, cellId);
                }

                long deviceId = suspiciousDevices.getLong("device_id");
                voltQueueSQL(createNewCohortMember, cellId, deviceId);
            }

            voltExecuteSQL();

        }

        return voltExecuteSQL(true);
    }

}
