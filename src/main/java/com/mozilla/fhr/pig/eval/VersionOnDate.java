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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.parser.ParserException;

import com.mozilla.util.DateUtil;

public class VersionOnDate extends EvalFunc<String> {

    static final String APPINFO_VERSIONS_FIELD = "org.mozilla.appInfo.versions";
    static final String V1_VERSION_FIELD = "version";
    static final String V2_VERSION_FIELD = "appVersion";
    static final String MULTI_VERSION_DELIMITER = "|";
    
    private Date refPerspectiveDate;
    private Map<Date, String> versionOnDateMap = Collections.emptyMap();
	private String dateFormat;
    
    public VersionOnDate(String dateFormat, String perspectiveDate) {
        try {
        	this.dateFormat = dateFormat;
        	if (perspectiveDate != null) {
        		refPerspectiveDate = DateUtil.parseAndCacheDate(dateFormat, perspectiveDate);
        	}
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid perspective date", e);
        }
    }
    
    @SuppressWarnings("unchecked")
	void parseInput(Tuple input) {
        if (input == null || input.size() == 0) {
        	versionOnDateMap = Collections.emptyMap();
            return;
        }
        
        try {
        	versionOnDateMap = new HashMap<Date, String>();
        	
            Map<String,Object> dataPoints = (Map<String,Object>)input.get(0);
            for (Map.Entry<String,Object> dayEntry : dataPoints.entrySet()) {
                Map<String,Object> dayMap = (Map<String,Object>)dayEntry.getValue();
                if (dayMap.containsKey(APPINFO_VERSIONS_FIELD)) {
                	Map<String,Object> appInfoVersionMap = (Map<String,Object>)dayMap.get(APPINFO_VERSIONS_FIELD);
                	Date date = DateUtil.parseAndCacheDate(dateFormat, dayEntry.getKey());
                	String version = null;
                	Integer documentVersion = getDocumentVersionFromMap(appInfoVersionMap);
                	if (documentVersion != null) {
                		switch(documentVersion) {
                		case 1: version = parse_v1(appInfoVersionMap); break;
                		case 2: version = parse_v2(appInfoVersionMap); break;
                		default:
                			throw new ParserException("Unsupported doc version " + documentVersion);
                		}

                		if (version != null) {
                			versionOnDateMap.put(date, version);
                		}
                	}
                	else {
            			throw new ParserException("Error parsing doc version");
                	}
                }
            }
        } catch (Exception e) {
            warn("Parse error: " + e.getMessage(), PigWarning.UDF_WARNING_1);
            versionOnDateMap = Collections.emptyMap();
            return;
        }
     }

    String getVersionOnDate(String str) {
		try {
			return getVersionOnDate(DateUtil.parseAndCacheDate(dateFormat, str));
		} catch (ParseException e) {
			return null;
		}
    }

    String getVersionOnDate(Date perspectiveDate) {
        String latestVersion = null;
        Date latestDate = null;
        if(perspectiveDate != null) {
            for (Date d : versionOnDateMap.keySet()) {
            	if (!d.after(perspectiveDate) && (latestDate==null || d.after(latestDate))) {
            		String version = versionOnDateMap.get(d);
            		latestDate = d;
            		latestVersion = version;
            	}
            }
        }
        return latestVersion;
    }
    
    @Override
    public String exec(Tuple input) throws IOException {
    	parseInput(input);
        return getVersionOnDate(refPerspectiveDate);
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
