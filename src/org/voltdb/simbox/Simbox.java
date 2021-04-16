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

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * 
 * Class to emulate a simbox
 *
 */
public class Simbox {

    /**
     * How many sims fit into the box, per manufacturer's data sheet.
     */
    public final static int SIMBOX_SIZE = 128;

    /**
     * HashMap containing sims
     */
    private HashMap<Long, UserDevice> sims = new HashMap<Long, UserDevice>();

    /**
     * Network cell we are currently in
     */
    private int cellId;

    /**
     * How many fraudulent calls we've made
     */
    private int evilCallCount = 0;
    
    /**
     * How many calls we made to ourselves so our sims look 'legit'
     */
    private int fakeCallCount = 0;
    
    /**
     * How many times we tried to make a call but couldn't because all our sims were
     * busy
     */
    private int busyCount = 0;

    /**
     * Percent of time we will fake a call between our sims instead of earning revenue
     */
    private final int fakeCallPct = 15;

    /**
     * Allow calls made to other sims in this box
     */
    private boolean selfCalls = false;

    /**
     * How many seconds have been spent on fraudulent calls
     */
    private long totalSimcallSeconds = 0;

    /**
     * Shared Random instance
     */
    private Random r = new Random();

    /**
     * Callback to make DB interaction run faster
     */
    private ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

    /**
     * Last time we moved cells.
     */
    private Date lastCellMove = new Date();

    /**
     * Rough guess as to how much profit we can make per call per minute.
     */
    private final float projectedProfitPerMinute = 0.16f;

    public Simbox(int cellId) {
        super();
        this.cellId = cellId;

    }

    /**
     * Add a sin to our simbox. Sims are picked randomly.
     * @param theDevice
     */
    public void addSim(UserDevice theDevice) {

        theDevice.setCellId(cellId);
        sims.put(theDevice.getDeviceId(), theDevice);

    }

    /**
     * 
     * @param mins
     * @return true if simbox hasn't changed cell in 'mins' minutes
     */
    public boolean haventMovedInXMinutes(int mins) {

        if (lastCellMove.getTime() + (60 * 1000 * mins) < System.currentTimeMillis()) {
            return true;
        }

        return false;
    }

    /**
     * Move all sims in this simbox to a new cell. This simulates the simbox being physically moved.
     * @param newCell
     * @param c
     * @throws NoConnectionsException
     * @throws IOException
     */
    public void moveCell(int newCell, Client c) throws NoConnectionsException, IOException {

        SimboxDataGenerator.msg("Moving " + sims.size() + " sims from cell " + cellId + " to " + newCell);

        cellId = newCell;

        ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

        Iterator<Map.Entry<Long, UserDevice>> iterator = sims.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Long, UserDevice> entry = iterator.next();
            c.callProcedure(coec, "ReportCellChange", entry.getValue().changeCellid(newCell));
        }

        lastCellMove = new Date();

    }

    /**
     * Make a call from a captive sim to a local number, while in fact connecting an 
     * incoming international connection.
     * @param calledNumber
     * @param client
     * @return 'true' if we made a call
     * @throws NoConnectionsException
     * @throws IOException
     */
    public boolean routeInternationalCall(UserDevice calledNumber, Client client)
            throws NoConnectionsException, IOException {

        if (selfCalls) {

            int fakeCallProbability = r.nextInt(100);

            if (fakeCallProbability <= fakeCallPct) {
                return makeFakeCall(client);
            }
        }

        int durationSeconds = r.nextInt(SimboxDataGenerator.MAX_RANDOM_CALL_LENGTH_SECONDS);

        UserDevice ud = getUnusedDevice();

        if (ud == null) {
            busyCount++;
            return false;
        }

        evilCallCount++;
        totalSimcallSeconds += durationSeconds;

        ud.makeCall(r, calledNumber, durationSeconds, client);

        return true;

    }

    /**
     * Setup a fake call between two of our sims, so the sims don't look quite so suspicious.
     * @param client
     * @return true if we able to make a call
     * @throws NoConnectionsException
     * @throws IOException
     */
    private boolean makeFakeCall(Client client) throws NoConnectionsException, IOException {

        int durationSeconds = r.nextInt(SimboxDataGenerator.MAX_RANDOM_CALL_LENGTH_SECONDS);

        UserDevice ud = getUnusedDevice();
        UserDevice fakeCaller = getUnusedDevice();

        if (ud == null || fakeCaller == null || ud.getDeviceId() == fakeCaller.getDeviceId()) {
            busyCount++;
            return false;
        }

        fakeCallCount++;

        fakeCaller.makeCall(r, ud, durationSeconds, client);

        return true;

    }

    /**
     * Find a sim that isn't in use.
     * 
     * @return Sim that isn't in use or null, if none can be found quickly
     */
    private UserDevice getUnusedDevice() {

        Object[] values = sims.values().toArray();

        // we use 'values.length * 2' as we need to search randomly but can't
        // spend forever doing so...
        for (int i = 0; i < (values.length * 2); i++) {
            UserDevice randomDevice = (UserDevice) values[r.nextInt(values.length)];

            if (!randomDevice.isBusy()) {
                return randomDevice;
            }
        }

        return null;
    }

    /**
     * @return How many sims we have
     */
    public int getSimCount() {
        return sims.size();
    }

    /**
     * Is this sim in the simbox?
     * @param deviceId
     * @return 'true' if it is.
     */
    public boolean isEvil(long deviceId) {

        UserDevice ud = sims.get(deviceId);

        if (ud == null) {
            return false;
        }

        return true;

    }

    /**
     * @return the cellId
     */
    public int getCellId() {
        return cellId;
    }

    /**
     * @return the selfCalls
     */
    public boolean isSelfCalls() {
        return selfCalls;
    }

    /**
     * @param selfCalls the selfCalls to set
     */
    public void setSelfCalls(boolean selfCalls) {
        this.selfCalls = selfCalls;
    }

    public float getProjectedProfit() {

        return (totalSimcallSeconds * projectedProfitPerMinute) / 60;

    }
    
    /**
     * @return list of keys
     */
    public long[] getSimList() {
         Object[] simKeys = sims.keySet().toArray();
         long[] simList =  new long[simKeys.length];
         
         for (int i=0; i < simKeys.length; i++) {
             simList[i] = ((Long)simKeys[i]).longValue();
         }
         
         return simList;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Simbox [cellId=");
        builder.append(cellId);
        builder.append(", size=");
        builder.append(sims.size());
        builder.append(", evilCallCount=");
        builder.append(evilCallCount);
        builder.append(", fakeCallCount=");
        builder.append(fakeCallCount);
        builder.append(", busyCount=");
        builder.append(busyCount);
        builder.append(", fakeCallPct=");
        builder.append(fakeCallPct);
        builder.append(", selfCalls=");
        builder.append(selfCalls);
        builder.append(", totalSimcallSeconds=");
        builder.append(totalSimcallSeconds);
        builder.append(", projectedProfit=");
        builder.append(getProjectedProfit());
        builder.append(", lastCellMove=");
        builder.append(lastCellMove);
        builder.append("]");
        return builder.toString();
    }

}
