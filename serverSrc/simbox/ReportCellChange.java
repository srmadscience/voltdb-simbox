package simbox;

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

/**
 * Report that a device has moved cells. This also updates the columns
 * cell_history_as_string_last3 and cell_history_as_string_last6 that are
 * used to spot cohorts of devices that move at the same time.
 *
 */
public class ReportCellChange extends VoltProcedure {

    // @formatter:off

	public static final SQLStmt getDevice = new SQLStmt(
			"SELECT cell_history_as_string FROM device_table WHERE device_id = ?;");

	public static final SQLStmt getCell = new SQLStmt("SELECT * FROM cell_table WHERE cell_id = ?;");

    public static final SQLStmt updateCurrentCell1 = new SQLStmt(
            "UPDATE device_table "
            + "SET current_cell_id = ? "
            + "  , cell_history_as_string = add_new_cell(cell_history_as_string, ?,?) "
            + "WHERE device_id = ?;");

    public static final SQLStmt updateCurrentCell2 = new SQLStmt(
            "UPDATE device_table "
            + "SET cell_history_as_string_last3 = get_last_n_cells(cell_history_as_string,3) "
            + "  , cell_history_as_string_last6 = get_last_n_cells(cell_history_as_string,6) "
            + "  , last_seen = NOW "
            + "WHERE device_id = ?;");
    
    public static final SQLStmt finishCurrentCellHist = new SQLStmt(
            "UPDATE device_cell_history "
            + "SET to_timestamp = DATEADD(MICROSECOND, -1, NOW) "
            + "WHERE device_id = ? "
            + "AND   to_timestamp = MAX_VALID_TIMESTAMP();");

    public static final SQLStmt createNewDeviceCellHist = new SQLStmt(
            "INSERT INTO device_cell_history "
            + "(device_id,current_cell_id,from_timestamp"
            + ",to_timestamp)"
            + "VALUES"
            + "(?,?,NOW,MAX_VALID_TIMESTAMP());");


	// @formatter:on

    public VoltTable[] run(long deviceId, long cellId) throws VoltAbortException {

        // See if we know about this user and cell...
        voltQueueSQL(getDevice, deviceId);
        voltQueueSQL(getCell, cellId);

        VoltTable[] deviceAndCell = voltExecuteSQL();

        // Sanity Check: Is this a real user?
        if (!deviceAndCell[0].advanceRow()) {
            throw new VoltAbortException("Device " + deviceId + " does not exist");
        }
        
        // Sanity Check: Is this a real cell?
        if (!deviceAndCell[1].advanceRow()) {
            throw new VoltAbortException("Cell " + cellId + " does not exist");
        }

        voltQueueSQL(updateCurrentCell1, cellId, cellId, this.getTransactionTime(), deviceId);
        voltQueueSQL(updateCurrentCell2, deviceId);
        voltQueueSQL(finishCurrentCellHist, deviceId);
        voltQueueSQL(createNewDeviceCellHist, deviceId,cellId);

        return voltExecuteSQL(true);
    }
}
