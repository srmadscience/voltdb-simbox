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


import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.types.TimestampType;

/**
 * Class containing methods that are turned into VoltDB functions.
 * <p>
 * A cell history entry is a : delimited list of cells and times. By looking 
 * at this list we can easily identify phones that move around as a group.
 *
 */
public class CellHistoryAsStringWrangler {

    /**
     * Record separator
     */
    private static final String COLON = ":";
    
    /**
     * Field separator
     */
    private static final char COMMA = ',';
    
    /**
     * Max number of elements in list
     */
    public static int MAX_LIST_LENGTH = 12;
    
    /**
     * Used for formatting messages
     */
    static SimpleDateFormat sdfDate = new SimpleDateFormat("mm");

    /**
     * Return the last 'n' elements of our list
     * @param currentList
     * @param cellCount 
     * @return a shortened list
     */
    public String getLastN(String currentList, int cellCount) {

        if (currentList == null || currentList.length() == 0) {
            return "";
        }

        String[] cellsSplit = currentList.split(COLON);

        // List does not need to be shortened
        if (cellsSplit.length <= cellCount) {
            return currentList;
        }

        // Create new list from last 'N' elements of old list
        StringBuffer b = new StringBuffer();

        for (int i = cellsSplit.length - cellCount; i < cellsSplit.length; i++) {
            b.append(cellsSplit[i]);
            b.append(COLON);
        }

        return b.toString();

    }

    /**
     * Add a new entry to a list
     * @param currentList
     * @param cellId
     * @param eventTime
     * @return A new list reporting a move to 'cellId' at 'eventTime'.
     */
    public String addNewCell(String currentList, long cellId, TimestampType eventTime) {
        
        String tempString = "";
        
        if (currentList != null) {
            tempString = new String(currentList);
        }
 
        String[] cellsSplit = tempString.split(COLON);

        // List will be too long if we add a cell - remove first entry
        if (cellsSplit.length + 1 > MAX_LIST_LENGTH) {
            tempString = getLastN(currentList, MAX_LIST_LENGTH - 1);
        }

        // Append our entry
        StringBuffer b = new StringBuffer(tempString);
        
        b.append(cellId);
        b.append(COMMA);       
        b.append(sdfDate.format(eventTime.asExactJavaDate()));
        b.append(COLON);
        
        return b.toString();

    }


}
