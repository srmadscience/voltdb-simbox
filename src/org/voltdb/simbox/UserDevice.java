package org.voltdb.simbox;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.types.TimestampType;

/**
 * A simulated phone on our network
 *
 */
public class UserDevice {

    /**
     * We make most of our calls to POPULAR_NUMBER_LIST_SIZE numbers...
     */
    private static final int POPULAR_NUMBER_LIST_SIZE = 10;
    
    /**
     * Probability of picking an entry
     */
    private static final int POPULAR_NUMBER_PCT = 30;

    /**
     * Phone ID
     */
    private long deviceId;
    
    /**
     * Current cell. 
     */
    private long cellId;
    
    /**
     * When phone was created. Older phones are less likely to be suspect.
     */
    private Date createDate = new Date();
    
    /**
     * When the current call ends. Will be in past if call is over.
     */
    private Date callEndTimeMs = new Date();
    
    /**
     * When the cell last moved.
     */
    private Date lastCellMove = new Date();
    
    /**
     * Used by async DB calls
     */
    private ComplainOnErrorCallback coec = new ComplainOnErrorCallback();
    
    /**
     * List of popular numbers. When asked to make a call we start at element 0 and 
     * if a random number <= 100 is < POPULAR_NUMBER_PCT we pick it. If we run off 
     * the end of the list we pick a number at random. Once populated around 98%
     * of calls will be to numbers on the list.
     */
    private ArrayList<Long> popularNumbers = new ArrayList<Long>(POPULAR_NUMBER_LIST_SIZE);

    /**
     * Create a device in a cell.
     * @param deviceId
     * @param cellId
     */
    public UserDevice(long deviceId, long cellId) {
        super();
        this.deviceId = deviceId;
        this.cellId = cellId;
    }

    /**
     * Make a call to another number. We assume someone else has checked to see
     * if this is a good idea.
     * 
     * @param r
     * @param calledNumber
     * @param durationSeconds
     * @param c
     * @throws NoConnectionsException
     * @throws IOException
     */
    public void makeCall(Random r, UserDevice calledNumber, int durationSeconds, Client c)
            throws NoConnectionsException, IOException {

        callEndTimeMs = new Date(System.currentTimeMillis() + (1000 * durationSeconds));
        TimestampType startTime = new TimestampType(new Date());

        long otherNumber = calledNumber.getDeviceId();
        String status = "E";

        // make sure called number adds calling number to its popular
        // numbers list where appropriate, and notes that it's now busy
        calledNumber.recordBeingCalled(this.getDeviceId(), durationSeconds);

        c.callProcedure(coec, "ReportDeviceActivity", deviceId, startTime, durationSeconds, "O", otherNumber, status);
        c.callProcedure(coec, "ReportDeviceActivity", otherNumber, startTime, durationSeconds, "I", deviceId, status);

    }

    /**
     * @return true if we in a call
     */
    public boolean isBusy() {

        if (callEndTimeMs.getTime() >= System.currentTimeMillis()) {
            return true;
        }

        return false;
    }

    /**
     * set new busy until time
     * @param callEndTime
     */
    public void areBusyUntil(Date callEndTime) {

        callEndTimeMs = callEndTime;

    }

    /**
     * 
     * @return correct set of parameters for a call to 'RegisterDevice'
     */
    public Object[] getParamsForRegisterProcedure() {

        Object[] params = { deviceId, cellId, createDate };
        return params;

    }

    /**
     * Change cell ID, and return correct set of params for a call to 'ReportCellChange'
     * @param newCellid
     * @return an Object[] contains parameters for a VoltDB call
     */
    public Object[] changeCellid(long newCellid) {

        setCellId(newCellid);

        Object[] params = { deviceId, cellId };
        return params;

    }

    /**
     * @return the deviceId
     */
    public long getDeviceId() {
        return deviceId;
    }

    /**
     * @return the cellId
     */
    public long getCellId() {
        return cellId;
    }

    /**
     * Uopdate the cell ID and when we did this
     * @param cellId
     */
    public void setCellId(long cellId) {
        this.cellId = cellId;
        lastCellMove = new Date();

    }

    /**
     * @param n minutes
     * @return true if we've been in a cell for at least 'n' minutes.
     */
    public boolean deviceInCellForNMinutes(int n) {

        if (lastCellMove.getTime() + (n * 60 * 1000) < System.currentTimeMillis()) {
            return true;
        }

        return false;
    }

    /**
     * @param createDate the createDate to set
     */
    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    /**
     * Get the nect number the device should call. Strong preference is given to
     * numbers we've called before. Otherwise we pick a random non-simbox one.
     * @param evilSimBox used so we can make sure new number is good
     * @param r Random
     * @param userCount range of possible numbers to call
     * @return
     */
    public long getNextNumberToCall(Simbox evilSimBox, Random r, int userCount) {

        // See if we can find a choice from our popular numbers list.
        // We start at the top and roll a dice each time...
        for (int i = 0; i < popularNumbers.size(); i++) {
            if (r.nextInt(100) <= POPULAR_NUMBER_PCT) {
                return popularNumbers.get(i);
            }
        }

        // We did't find a popular number. Pick one randomly. This will
        // work provided the number of devices in the simbox is < the total number of devices.
        while (true) {

            long newNumber = r.nextInt(userCount);

            if (!evilSimBox.isEvil(newNumber) && (newNumber != deviceId)) {

                // Add new number to our list
                if (popularNumbers.size() < POPULAR_NUMBER_LIST_SIZE) {
                    popularNumbers.add(newNumber);

                }

                return newNumber;
            }

        }

    }

    /**
     * Record the fact that someone is calling us, so we know we're busy and can't make a call
     * of our own.
     * @param proposedDeviceId
     * @param duration
     */
    public void recordBeingCalled(long callingDeviceId, int duration) {

        callEndTimeMs = new Date(System.currentTimeMillis() + (1000 * duration));

        // Add number to our popular number list if it has space. This means that the first
        // people we tend to call are they first people who called us...
        if (popularNumbers.size() < POPULAR_NUMBER_LIST_SIZE && deviceId != callingDeviceId) {
            popularNumbers.add(callingDeviceId);
        }
    }

}
