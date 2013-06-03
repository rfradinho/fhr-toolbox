/*
 * Copyright 2012 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.fhr.pig.eval;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class VersionOnDate extends EvalFunc<String> {

    static final String APPINFO_VERSIONS_FIELD = "org.mozilla.appInfo.versions";
    static final String V1_VERSION_FIELD = "version";
    static final String V2_VERSION_FIELD = "appVersion";
    static final String MULTI_VERSION_DELIMITER = "|";
    
    private final SimpleDateFormat sdf;
    private long perspectiveTime;
    
    public VersionOnDate(String dateFormat, String perspectiveDate) {
        sdf = new SimpleDateFormat(dateFormat);
        try {
            Date d = sdf.parse(perspectiveDate);
            perspectiveTime = d.getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid perspective date", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        String latestVersion = null;
        long latestTime = 0;
        try {
            Map<String,Object> dataPoints = (Map<String,Object>)input.get(0);
            for (Map.Entry<String,Object> dayEntry : dataPoints.entrySet()) {
                Map<String,Object> dayMap = (Map<String,Object>)dayEntry.getValue();
                if (dayMap.containsKey(APPINFO_VERSIONS_FIELD)) {
                    Date d = sdf.parse(dayEntry.getKey());
                    if (d.getTime() <= perspectiveTime && d.getTime() > latestTime) {
                        Map<String,Object> appInfoVersionMap = (Map<String,Object>)dayMap.get(APPINFO_VERSIONS_FIELD);
                        String version = null;
                        
                        Integer documentVersion = getDocumentVersionFromMap(appInfoVersionMap);
                        if (documentVersion != null) {
                            switch(documentVersion) {
                            case 1: version = parse_v1(appInfoVersionMap); break;
                            case 2: version = parse_v2(appInfoVersionMap); break;
                            default: 
                                warn("Unsupported doc version " + documentVersion, PigWarning.UDF_WARNING_1);
                                return null;
                            }
                            
                            if (version != null) {
                                latestTime = d.getTime();
                                latestVersion = version;
                            }
                        }
                        else {
                            warn( "Error parsing doc version", PigWarning.UDF_WARNING_1);
                        }
                        
                    }
                }
            }
        } catch (Exception e) {
            warn("Parse error: " + e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
        }
        return latestVersion;
    }

    Integer getDocumentVersionFromMap(Map<String, Object> appInfoVersionMap) {
        Integer ret = null;
        try {
            ret = (Integer)appInfoVersionMap.get("_v");
        } catch (Exception e) {
            // ignore - return null
        }
        return ret;
    }

    String parse_v1(Map<String, Object> appInfoVersionMap) throws ExecException {
        StringBuilder sb = null;
        if (appInfoVersionMap.containsKey(V1_VERSION_FIELD)) {
            DataBag versionBag = (DataBag)appInfoVersionMap.get(V1_VERSION_FIELD);
            sb = getVersionsFromArray(versionBag);
        }
        return (sb!=null && sb.length()>0) ? sb.toString() : null;
    }

    String parse_v2(Map<String, Object> appInfoVersionMap) throws ExecException {
        StringBuilder sb = null;
        if (appInfoVersionMap.containsKey(V2_VERSION_FIELD)) {
            DataBag versionBag = (DataBag)appInfoVersionMap.get(V2_VERSION_FIELD);
            sb = getVersionsFromArray(versionBag);
        }
        return (sb!=null && sb.length()>0) ? sb.toString() : null;
    }

    StringBuilder getVersionsFromArray(DataBag versionBag) throws ExecException {
        StringBuilder sb = new StringBuilder();
        Iterator<Tuple> vbIter = versionBag.iterator();
        for (int i=0; i < versionBag.size() && vbIter.hasNext(); i++) {
            Tuple versionTuple = vbIter.next();
            for (int versionTupleIdx=0; versionTupleIdx<versionTuple.size(); versionTupleIdx++) {
                if (sb.length()>0) {
                    sb.append(MULTI_VERSION_DELIMITER);
                }
                sb.append(versionTuple.get(versionTupleIdx));
            }
        }
        return sb;
    }

    
}
