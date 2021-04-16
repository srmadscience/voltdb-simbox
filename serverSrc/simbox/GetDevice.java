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
public class GetDevice extends VoltProcedure {

    // @formatter:off

	public static final SQLStmt getDevice = new SQLStmt(
			"SELECT * FROM device_table WHERE device_id = ?;");

    public static final SQLStmt getDeviceCellHistory = new SQLStmt(
            "SELECT * FROM device_cell_history WHERE device_id = ? ORDER BY from_timestamp;");
    
    public static final SQLStmt getIncomingCallHistory = new SQLStmt(
            "SELECT * FROM device_incoming_call_history WHERE device_id = ? ORDER BY START_TIME;");
    
    public static final SQLStmt getOutgoingCallHistory = new SQLStmt(
            "SELECT * FROM device_outgoing_call_history WHERE device_id = ? ORDER BY START_TIME;");

 	// @formatter:on

    /**
     * A VoltDB stored procedure to get a device and its data
     * @param deviceId
     * @return Device info
     * @throws VoltAbortException
     */
    public VoltTable[] run(long deviceId) throws VoltAbortException {
  
        voltQueueSQL(getDevice, deviceId);
        voltQueueSQL(getDeviceCellHistory, deviceId);
        voltQueueSQL(getIncomingCallHistory, deviceId);
        voltQueueSQL(getOutgoingCallHistory, deviceId);

        return voltExecuteSQL(true);
    }
}
