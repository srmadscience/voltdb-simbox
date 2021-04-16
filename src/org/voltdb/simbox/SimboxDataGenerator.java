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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

/**
 * 
 *
 */
public class SimboxDataGenerator {

    /**
     * How many times we check a random device in our hashmap to see if it is busy.
     * If the map is 95% busy 30 attempts will have a 79% chance of finding a
     * non-busy device.
     * 
     * Making this number bigger will slow the generator down as the system gets
     * busy.
     */
    private static final int RANDOM_SEARCH_ATTEMPTS = 30;

    /**
     * One day in milliseconds
     */
    private static final int ONE_DAY_IN_MS = 1000 * 60 * 60 * 24;

    /**
     * One year in milliseconds
     */
    private static final int ONE_YEAR_IN_MS = ONE_DAY_IN_MS * 365;

    /**
     * Used for formatting messages
     */
    static SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Our handle to VoltDB
     */
    Client voltClient = null;

    /**
     * Comma delimited list of hosts *without* port numbers.
     */
    String hostnames;

    /**
     * How many sessions / users to create. You can create more than 1 session per
     * user by running multiple instances and using the 'offset' parameter
     */
    int userCount;

    /**
     * Target transactions per millisecond
     */
    int tpMs;

    /**
     * How many seconds to run for.
     */
    int durationSeconds;

    /**
     * How many network 'cells' to have
     */
    int cellCount;

    /**
     * HashMap containing our sessions. It will get to be big...
     */
    HashMap<Long, UserDevice> sessionMap;

    /**
     * A representation of a simbox
     */
    Simbox evilSimBox;

    /**
     * Shared Random instance.
     */
    Random r = new Random();

    /**
     * UTC time we started running
     */
    long startMs;

    /**
     * Maximum length of a call. It's artificially short.
     */
    public static int MAX_RANDOM_CALL_LENGTH_SECONDS = 60;

    /**
     * Run a simuation of a phone system where we aim to detect a simbox.
     * 
     * @param hostnames
     * @param userCount
     * @param tpMs
     * @param durationSeconds
     * @param cellCount
     * @throws Exception
     */
    public SimboxDataGenerator(String hostnames, int userCount, int tpMs, int durationSeconds, int cellCount)
            throws Exception {

        this.hostnames = hostnames;
        this.userCount = userCount;
        this.tpMs = tpMs;
        this.durationSeconds = durationSeconds;
        this.cellCount = cellCount;

        evilSimBox = new Simbox(0);
        sessionMap = new HashMap<Long, UserDevice>(userCount);

        SimboxDataGenerator.msg("hostnames=" + hostnames + ", users=" + userCount + ", tpMs=" + tpMs
                + ",durationSeconds=" + durationSeconds + ", cellCount=" + cellCount);

        SimboxDataGenerator.msg("Log into VoltDB");
        voltClient = connectVoltDB(hostnames);

    }

    /**
     * Run our simulation...
     */
    public void run() {

        try {
            long laststatstime = System.currentTimeMillis();

            long currentMs = System.currentTimeMillis();
            int tpThisMs = 0;

            int skipCount = 0;
            int busyCount = 0;
            int evilCount = 0;
            int goodCallCount = 0;
            int goodCellMoves = 0;
            int evilCellMoves = 0;

            SimboxDataGenerator.msg("Creating cells");

            ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

            voltClient.callProcedure("@AdHoc", "DELETE FROM cell_suspicious_cohort_members;");
            voltClient.callProcedure("@AdHoc", "DELETE FROM cell_suspicious_cohorts;");

            // Create cells
            for (int i = 0; i < cellCount; i++) {
                voltClient.callProcedure(coec, "cell_table.UPSERT", i);
            }

            voltClient.drain();

            // Create devices...
            SimboxDataGenerator.msg("Creating " + userCount + " devices");

            for (int i = 0; i < userCount; i++) {

                UserDevice ud = new UserDevice(i, i % cellCount);

                int createDateInPastMs = r.nextInt(ONE_YEAR_IN_MS);

                // Add 1 in 100 newly created sims to the simbox until it's full
                if (evilSimBox.getSimCount() < Simbox.SIMBOX_SIZE && r.nextInt(100) == 0) {
                    createDateInPastMs = r.nextInt(ONE_DAY_IN_MS);
                    evilSimBox.addSim(ud);
                }

                Date createDate = new Date(System.currentTimeMillis() - createDateInPastMs);
                ud.setCreateDate(createDate);

                voltClient.callProcedure(coec, "RegisterDevice", ud.getParamsForRegisterProcedure());
                sessionMap.put(ud.getDeviceId(), ud);

            }

            final long[] simBoxIds = evilSimBox.getSimList();

            // Move devices around a bit...
            int moveCount = 6;
            SimboxDataGenerator.msg("Moving " + userCount + " devices " + moveCount + " times...");

            for (int j = 0; j < moveCount; j++) {

                SimboxDataGenerator.msg("Move " + (j + 1));

                for (int i = 0; i < userCount; i++) {

                    UserDevice ourSession = sessionMap.get((long) i);
                    voltClient.callProcedure(coec, "ReportCellChange", ourSession.changeCellid(r.nextInt(cellCount)));
                    goodCellMoves++;
                }

            }

            voltClient.drain();

            SimboxDataGenerator
                    .msg("Created " + userCount + " devices, " + evilSimBox.getSimCount() + " are in a sim box");

            SimboxDataGenerator.msg("Run started");
            startMs = System.currentTimeMillis();
            laststatstime = System.currentTimeMillis();

            while (System.currentTimeMillis() < (startMs + (1000 * durationSeconds))) {

                // See if the simbox has capacity to make a call...
                UserDevice callingNumber = getNonbusyLegalNumber(null);
                UserDevice calledNumber = getNonbusyLegalNumber(callingNumber);

                if (callingNumber == null || calledNumber == null) {
                    // Can't find a free number
                    busyCount++;
                } else {

                    // See if the simbox has capacity to make a call, and if so, make one...
                    if (evilSimBox.routeInternationalCall(calledNumber, voltClient)) {
                        evilCount++;
                    } else {

                        // Do 'normal' activity.

                        // Change cell one time in 20.
                        if (callingNumber.deviceInCellForNMinutes(2) && r.nextInt(20) == 0) {

                            // Change cell id

                            long cellId = callingNumber.getCellId();

                            // Move to an adjacent cell...
                            if (r.nextInt(2) == 0) {
                                cellId = (cellId + 1) % cellCount;
                            } else {

                                if (cellId == 0) {
                                    cellId = cellCount - 1;
                                } else {
                                    cellId -= 1;
                                }

                            }

                            voltClient.callProcedure(coec, "ReportCellChange",
                                    callingNumber.changeCellid(r.nextInt(cellCount)));
                            goodCellMoves++;
                            tpThisMs++;

                        } else {

                            // make a normal call

                            int callLength = r.nextInt(MAX_RANDOM_CALL_LENGTH_SECONDS);

                            callingNumber.makeCall(r, calledNumber, callLength, voltClient);

                            tpThisMs += 2;
                            goodCallCount++;

                        }
                    }

                    // Our evil simbox is in the back of a truck and moves around...
                    if (evilSimBox.haventMovedInXMinutes(2)) {

                        int nextEvilCellId = (evilSimBox.getCellId() + 1) % cellCount;

                        evilSimBox.moveCell(nextEvilCellId, voltClient);
                        tpThisMs += evilSimBox.getSimCount();
                        evilCellMoves += evilSimBox.getSimCount();
                        ;
                    }

                    // control number of calls per millisecond
                    if (tpThisMs > tpMs) {

                        // but sleep if we're moving too fast...
                        while (currentMs == System.currentTimeMillis()) {
                            try {
                                Thread.sleep(0, 50000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        currentMs = System.currentTimeMillis();
                        tpThisMs = 0;
                    }

                    // Every 60 seconds dump stats to console...
                    if (laststatstime + 60000 < System.currentTimeMillis()) {

                        if (getParam("ENABLE_SUSPICOUS_COHORT_DETECTION", 0, voltClient) == 1) {
                            voltClient.callProcedure("NoteSuspiciousCohort");
                        }

                        if (getParam("SIMBOX_CALLS_ITSELF", 0, voltClient) == 1) {
                            evilSimBox.setSelfCalls(true);
                        } else {
                            evilSimBox.setSelfCalls(false);
                        }

                        double tps = (1000 * goodCallCount) / (System.currentTimeMillis() - laststatstime);

                        SimboxDataGenerator.msg("Calls Per Second=" + (long) tps);
                        SimboxDataGenerator.msg("Active Sessions: " + sessionMap.size());
                        SimboxDataGenerator.msg("skipCount = " + skipCount);
                        SimboxDataGenerator.msg("busyCount = " + busyCount);
                        SimboxDataGenerator.msg("evilCount = " + evilCount);
                        SimboxDataGenerator.msg("goodCallCount = " + goodCallCount);
                        SimboxDataGenerator.msg("goodCellMoves = " + goodCellMoves);
                        SimboxDataGenerator.msg("evilCellMoves = " + evilCellMoves);
                        SimboxDataGenerator.msg(evilSimBox.toString());

                        reportStat("tps", (long) tps, voltClient);
                        reportStat("sessions", sessionMap.size(), voltClient);
                        reportStat("goodCallCount", goodCallCount, voltClient);
                        reportStat("evilCount", evilCount, voltClient);
                        reportStat("busyCount", busyCount, voltClient);
                        reportStat("goodCellMoves", goodCellMoves, voltClient);
                        reportStat("evilCellMoves", evilCellMoves, voltClient);
                        reportStat("evilRevenueCents", (long) (evilSimBox.getProjectedProfit() * 100), voltClient);

                        
                        zeroStats(voltClient);
                        
                        ClientResponse cr = voltClient.callProcedure("getSuspectStatus", simBoxIds);
                        if (cr.getStatus() == ClientResponse.SUCCESS) {
                            VoltTable resultsTable = cr.getResults()[0];

                            while (resultsTable.advanceRow()) {
                                String suspiciousBecause = resultsTable.getString("suspicious_because");
                                if (suspiciousBecause == null) {
                                    suspiciousBecause = "not_suspected";
                                }

                                long suspiciousCount = resultsTable.getLong("how_many");

                                reportStat("simboxstatus_" + suspiciousBecause, suspiciousCount, voltClient);

                            }
                        }

                        laststatstime = System.currentTimeMillis();
                        skipCount = 0;
                        busyCount = 0;
                        evilCount = 0;
                        goodCallCount = 0;
                        goodCellMoves = 0;
                        evilCellMoves = 0;

                        printApplicationStats(voltClient);

                    }

                }

            }

            SimboxDataGenerator.msg("Run finished; ending sessions");

            laststatstime = System.currentTimeMillis();

            try {
                voltClient.drain();
            } catch (Exception e) {
                SimboxDataGenerator.msg(e);
            }

            SimboxDataGenerator.msg("done...");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Get a number which isn't currently involved in making a call../
     * 
     * @param callingNumber the number we're calling from
     * @return A number we can call...
     */
    private UserDevice getNonbusyLegalNumber(UserDevice callingNumber) {

        for (int i = 0; i < RANDOM_SEARCH_ATTEMPTS; i++) {

            if (callingNumber == null) {

                long nonBusyNumber = r.nextInt(userCount);

                if (!evilSimBox.isEvil(nonBusyNumber)) {

                    UserDevice ourSession = sessionMap.get(nonBusyNumber);

                    if (!ourSession.isBusy()) {
                        return ourSession;
                    }

                }

            } else {

                long nextNumberToCall = callingNumber.getNextNumberToCall(evilSimBox, r, userCount);

                UserDevice ourSession = sessionMap.get(nextNumberToCall);

                if (!ourSession.isBusy()) {
                    return ourSession;
                }

            }
        }

        return null;
    }

    /**
     * Store a statistic
     * 
     * @param statName
     * @param statValue
     * @param c
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private static void reportStat(String statName, long statValue, Client c)
            throws NoConnectionsException, IOException, ProcCallException {

        c.callProcedure("@AdHoc", "UPSERT INTO simbox_stats " + "(stat_name, stat_value) " + "VALUES ('" + statName
                + "'," + statValue + ");");

    }

    /**
     * zero statistics
     * 
     * @param c
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private static void zeroStats(Client c)
            throws NoConnectionsException, IOException, ProcCallException {

        c.callProcedure("@AdHoc", "UPDATE simbox_stats SET stat_value = 0 WHERE stat_name like 'simboxstatus%';");

    }

    
    
    
    /**
     * 
     * Get a parameter
     * 
     * @param paramName
     * @param defaultValue
     * @param c
     * @return parameter value or 'defaultValue' if not set
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private static long getParam(String paramName, long defaultValue, Client c)
            throws NoConnectionsException, IOException, ProcCallException {

        ClientResponse cr = c.callProcedure("@AdHoc",
                "SELECT parameter_value FROM simbox_parameters WHERE parameter_name = '" + paramName + "';");

        if (cr.getResults()[0].advanceRow()) {
            long newValue = cr.getResults()[0].getLong("parameter_value");
            return newValue;
        }
        return defaultValue;

    }
    
    
    
    /**
     * Run from command line
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            SimboxDataGenerator.msg("Usage: SimboxDataGenerator hostnames userCount tpMs durationSeconds cellCount ");
            System.exit(1);
        }

        String hostnames = args[0];
        int userCount = Integer.parseInt(args[1]);
        int tpMs = Integer.parseInt(args[2]);
        int durationSeconds = Integer.parseInt(args[3]);
        int cellCount = Integer.parseInt(args[4]);

        msg("[hostnames userCount tpMs durationSeconds cellCount ]=" + Arrays.toString(args));
        SimboxDataGenerator pdg = new SimboxDataGenerator(hostnames, userCount, tpMs, durationSeconds, cellCount);

        pdg.run();

    }

    /**
     * 
     * Connect to VoltDB using native APIS
     * 
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            SimboxDataGenerator.msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (int i = 0; i < hostnameArray.length; i++) {
                SimboxDataGenerator.msg("Connect to " + hostnameArray[i] + "...");
                try {
                    client.createConnection(hostnameArray[i]);
                } catch (Exception e) {
                    SimboxDataGenerator.msg(e.getMessage());
                }
            }

            if (client.getConnectedHostList().size() == 0) {
                throw new Exception("No hosts usable...");
            }

            SimboxDataGenerator.msg("Connected to VoltDB");

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    /**
     * Check VoltDB to see how things are going...
     * 
     * @param client
     * @param executiveSession
     * @param averageSession
     * @param studentSession
     */
    public static void printApplicationStats(Client client) {

        SimboxDataGenerator.msg("");
        SimboxDataGenerator.msg("Latest Stats:");

        try {
            ClientResponse cr = client.callProcedure("ShowSimboxActivity__promBL");
            if (cr.getStatus() == ClientResponse.SUCCESS) {
                VoltTable[] resultsTables = cr.getResults();
                for (int i = 0; i < resultsTables.length; i++) {
                    if (resultsTables[i].advanceRow()) {
                        SimboxDataGenerator.msg(resultsTables[i].toFormattedString());
                    }

                }

            }
        } catch (IOException | ProcCallException e) {

            e.printStackTrace();
        }

    }

    /**
     * Print a formatted message.
     * 
     * @param message
     */
    public static void msg(String message) {

        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Print a formatted message.
     * 
     * @param e
     */
    public static void msg(Exception e) {

        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + e.getClass().getName() + ":" + e.getMessage());

    }

}
