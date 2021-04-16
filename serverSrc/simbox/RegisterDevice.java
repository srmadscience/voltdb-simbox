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
import org.voltdb.types.TimestampType;

/**
 * Create a new device in our system.
 *
 */
public class RegisterDevice extends VoltProcedure {

    // @formatter:off

	public static final SQLStmt getDevice = new SQLStmt(
			"SELECT device_id FROM device_table WHERE device_id = ?;");

	public static final SQLStmt getCell = new SQLStmt("SELECT * FROM cell_table WHERE cell_id = ?;");

    public static final SQLStmt createNewDevice = new SQLStmt(
            "INSERT INTO device_table "
            + "(device_id,current_cell_id,first_seen,last_seen"
            + ",cell_history_as_string,cell_history_as_string_last3,cell_history_as_string_last6"
            + ",suspicious_because,suspicious_value)"
            + "VALUES"
            + "(?,?,?,NOW"
            + ",add_new_cell(null, ?,NOW),add_new_cell(null, ?,NOW),add_new_cell(null, ?,NOW) "
            + ",null,null);");

    public static final SQLStmt createNewDeviceCellHist = new SQLStmt(
            "INSERT INTO device_cell_history "
            + "(device_id,current_cell_id,from_timestamp"
            + ",to_timestamp)"
            + "VALUES"
            + "(?,?,NOW,MAX_VALID_TIMESTAMP());");
	
    public static final SQLStmt removeDevice = new SQLStmt(
            "DELETE FROM device_table WHERE device_id = ?;");
    
    public static final SQLStmt removeDeviceCellHistory = new SQLStmt(
            "DELETE FROM device_cell_history WHERE device_id = ?;");
    
    public static final SQLStmt removeIncomingCallHistory = new SQLStmt(
            "DELETE FROM device_incoming_call_history WHERE device_id = ?;");
    
    public static final SQLStmt removeOutgoingCallHistory = new SQLStmt(
            "DELETE FROM device_outgoing_call_history WHERE device_id = ?;");

 	// @formatter:on

    /**
     * Register a device
     * 
     * @param deviceId
     * @param cellId
     * @param createDate
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(long deviceId, long cellId, TimestampType createDate) throws VoltAbortException {


        // See if we know about this user and transaction...
        voltQueueSQL(getDevice, deviceId);
        voltQueueSQL(getCell, cellId);

        VoltTable[] deviceAndCell = voltExecuteSQL();

        // Sanity Check: Is this a real cell?
        if (!deviceAndCell[1].advanceRow()) {
            throw new VoltAbortException("Cell " + cellId + " does not exist");
        }

        // Sanity Check: Is this a real user?
        if (deviceAndCell[0].advanceRow()) {
            voltQueueSQL(removeDevice, deviceId);
            voltQueueSQL(removeDeviceCellHistory, deviceId);
            voltQueueSQL(removeIncomingCallHistory, deviceId);
            voltQueueSQL(removeOutgoingCallHistory, deviceId);
        }

        voltQueueSQL(createNewDevice, deviceId, cellId, createDate, cellId, cellId, cellId);
        voltQueueSQL(createNewDeviceCellHist, deviceId, cellId);

        return voltExecuteSQL(true);
    }
}
