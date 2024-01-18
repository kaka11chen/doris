// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common.profile;

import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SummaryProfile is part of a query profile.
 * It contains the summary information of a query.
 */
public class SummaryProfile {
    // Summary
    public static final String SUMMARY_PROFILE_NAME = "Summary";
    public static final String PROFILE_ID = "Profile ID";
    public static final String DORIS_VERSION = "Doris Version";
    public static final String TASK_TYPE = "Task Type";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String TASK_STATE = "Task State";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String IS_CACHED = "Is Cached";
    public static final String IS_NEREIDS = "Is Nereids";
    public static final String IS_PIPELINE = "Is Pipeline";
    public static final String TOTAL_INSTANCES_NUM = "Total Instances Num";
    public static final String INSTANCES_NUM_PER_BE = "Instances Num Per BE";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE = "Parallel Fragment Exec Instance Num";
    public static final String TRACE_ID = "Trace ID";
    public static final String WORKLOAD_GROUP = "Workload Group";

    // Execution Summary
    public static final String EXECUTION_SUMMARY_PROFILE_NAME = "Execution Summary";
    public static final String ANALYSIS_TIME = "Analysis Time";
    public static final String JOIN_REORDER_TIME = "JoinReorder Time";
    public static final String CREATE_SINGLE_NODE_TIME = "CreateSingleNode Time";
    public static final String QUERY_DISTRIBUTED_TIME = "QueryDistributed Time";
    public static final String INIT_SCAN_NODE_TIME = "Init Scan Node Time";
    public static final String FINALIZE_SCAN_NODE_TIME = "Finalize Scan Node Time";
    public static final String GET_SPLITS_TIME = "Get Splits Time";
    public static final String GET_PARTITIONS_TIME = "Get PARTITIONS Time";
    public static final String GET_PARTITION_FILES_TIME = "Get PARTITION FILES Time";
    public static final String CREATE_SCAN_RANGE_TIME = "Create Scan Range Time";

    public static final String PLAN_TIME = "Plan Time";
    public static final String SCHEDULE_TIME = "Schedule Time";
    public static final String FETCH_RESULT_TIME = "Fetch Result Time";
    public static final String WRITE_RESULT_TIME = "Write Result Time";
    public static final String WAIT_FETCH_RESULT_TIME = "Wait and Fetch Result Time";

    public static final String NEREIDS_ANALYSIS_TIME = "Nereids Analysis Time";
    public static final String NEREIDS_REWRITE_TIME = "Nereids Rewrite Time";
    public static final String NEREIDS_OPTIMIZE_TIME = "Nereids Optimize Time";
    public static final String NEREIDS_TRANSLATE_TIME = "Nereids Translate Time";

    public static final String COMPUTE_ASSIGNMENT_TIME = "Compute Assignment Time";

    public static final String COMPUTE_ASSIGNMENT_INFO = "Compute Assignment Info";

    // These info will display on FE's web ui table, every one will be displayed as
    // a column, so that should not
    // add many columns here. Add to ExcecutionSummary list.
    public static final ImmutableList<String> SUMMARY_KEYS = ImmutableList.of(PROFILE_ID, TASK_TYPE,
            START_TIME, END_TIME, TOTAL_TIME, TASK_STATE, USER, DEFAULT_DB, SQL_STATEMENT);

    public static final ImmutableList<String> EXECUTION_SUMMARY_KEYS = ImmutableList.of(
            NEREIDS_ANALYSIS_TIME, NEREIDS_REWRITE_TIME, NEREIDS_OPTIMIZE_TIME, NEREIDS_TRANSLATE_TIME,
            WORKLOAD_GROUP, ANALYSIS_TIME,
            PLAN_TIME, JOIN_REORDER_TIME, CREATE_SINGLE_NODE_TIME, QUERY_DISTRIBUTED_TIME,
            INIT_SCAN_NODE_TIME, FINALIZE_SCAN_NODE_TIME, GET_SPLITS_TIME, GET_PARTITIONS_TIME,
            GET_PARTITION_FILES_TIME, CREATE_SCAN_RANGE_TIME, COMPUTE_ASSIGNMENT_TIME, COMPUTE_ASSIGNMENT_INFO, SCHEDULE_TIME, FETCH_RESULT_TIME,
            WRITE_RESULT_TIME, WAIT_FETCH_RESULT_TIME, DORIS_VERSION, IS_NEREIDS, IS_PIPELINE,
            IS_CACHED, TOTAL_INSTANCES_NUM, INSTANCES_NUM_PER_BE, PARALLEL_FRAGMENT_EXEC_INSTANCE, TRACE_ID);

    // Ident of each item. Default is 0, which doesn't need to present in this Map.
    // Please set this map for new profile items if they need ident.
    public static ImmutableMap<String, Integer> EXECUTION_SUMMARY_KEYS_IDENTATION;
    static {
        EXECUTION_SUMMARY_KEYS_IDENTATION = ImmutableMap.<String, Integer>builder()
                .put(JOIN_REORDER_TIME, 1)
                .put(CREATE_SINGLE_NODE_TIME, 1)
                .put(QUERY_DISTRIBUTED_TIME, 1)
                .put(INIT_SCAN_NODE_TIME, 1)
                .put(FINALIZE_SCAN_NODE_TIME, 1)
                .put(GET_SPLITS_TIME, 2)
                .put(GET_PARTITIONS_TIME, 3)
                .put(GET_PARTITION_FILES_TIME, 3)
                .put(CREATE_SCAN_RANGE_TIME, 2)
                .put(COMPUTE_ASSIGNMENT_TIME, 3)
                .put(COMPUTE_ASSIGNMENT_INFO, 3)
                .build();
    }

    private RuntimeProfile summaryProfile;
    private RuntimeProfile executionSummaryProfile;

    private long nereidsAnalysisFinishTime = -1;
    private long nereidsRewriteFinishTime = -1;
    private long nereidsOptimizeFinishTime = -1;
    private long nereidsTranslateFinishTime = -1;
    // timestamp of query begin
    private long queryBeginTime = -1;
    // Analysis end time
    private long queryAnalysisFinishTime = -1;
    // Join reorder end time
    private long queryJoinReorderFinishTime = -1;
    // Create single node plan end time
    private long queryCreateSingleNodeFinishTime = -1;
    // Create distribute plan end time
    private long queryDistributedFinishTime = -1;
    private long initScanNodeStartTime = -1;
    private long initScanNodeFinishTime = -1;
    private long finalizeScanNodeStartTime = -1;
    private long finalizeScanNodeFinishTime = -1;
    private long getSplitsStartTime = -1;
    private long getPartitionsFinishTime = -1;
    private long getPartitionFilesFinishTime = -1;
    private long getSplitsFinishTime = -1;
    private long createScanRangeFinishTime = -1;
    // Plan end time
    private long queryPlanFinishTime = -1;
    // Fragment schedule and send end time
    private long queryScheduleFinishTime = -1;
    // Query result fetch end time
    private long queryFetchResultFinishTime = -1;
    private long tempStarTime = -1;
    private long queryFetchResultConsumeTime = 0;
    private long queryWriteResultConsumeTime = 0;

    private long computeAssignmentTime = 0;

    private Multimap<Backend, Split> assignment;



    public SummaryProfile(RuntimeProfile rootProfile) {
        summaryProfile = new RuntimeProfile(SUMMARY_PROFILE_NAME);
        executionSummaryProfile = new RuntimeProfile(EXECUTION_SUMMARY_PROFILE_NAME);
        init();
        rootProfile.addChild(summaryProfile);
        rootProfile.addChild(executionSummaryProfile);
    }

    private void init() {
        for (String key : SUMMARY_KEYS) {
            summaryProfile.addInfoString(key, "N/A");
        }
        for (String key : EXECUTION_SUMMARY_KEYS) {
            executionSummaryProfile.addInfoString(key, "N/A");
        }
    }

    public void prettyPrint(StringBuilder builder) {
        summaryProfile.prettyPrint(builder, "");
        executionSummaryProfile.prettyPrint(builder, "");
    }

    public Map<String, String> getAsInfoStings() {
        Map<String, String> infoStrings = Maps.newHashMap();
        for (String header : SummaryProfile.SUMMARY_KEYS) {
            infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        return infoStrings;
    }

    public void update(Map<String, String> summaryInfo) {
        updateSummaryProfile(summaryInfo);
        updateExecutionSummaryProfile();
    }

    private void updateSummaryProfile(Map<String, String> infos) {
        for (String key : infos.keySet()) {
            if (SUMMARY_KEYS.contains(key)) {
                summaryProfile.addInfoString(key, infos.get(key));
            } else if (EXECUTION_SUMMARY_KEYS.contains(key)) {
                // Some static value is build in summary profile, should add
                // them to execution summary profile during update.
                executionSummaryProfile.addInfoString(key, infos.get(key));
            }
        }
    }

    private void updateExecutionSummaryProfile() {
        executionSummaryProfile.addInfoString(NEREIDS_ANALYSIS_TIME, getPrettyNereidsAnalysisTime());
        executionSummaryProfile.addInfoString(NEREIDS_REWRITE_TIME, getPrettyNereidsRewriteTime());
        executionSummaryProfile.addInfoString(NEREIDS_OPTIMIZE_TIME, getPrettyNereidsOptimizeTime());
        executionSummaryProfile.addInfoString(NEREIDS_TRANSLATE_TIME, getPrettyNereidsTranslateTime());
        executionSummaryProfile.addInfoString(ANALYSIS_TIME, getPrettyQueryAnalysisFinishTime());
        executionSummaryProfile.addInfoString(PLAN_TIME, getPrettyQueryPlanFinishTime());
        executionSummaryProfile.addInfoString(JOIN_REORDER_TIME, getPrettyQueryJoinReorderFinishTime());
        executionSummaryProfile.addInfoString(CREATE_SINGLE_NODE_TIME, getPrettyCreateSingleNodeFinishTime());
        executionSummaryProfile.addInfoString(QUERY_DISTRIBUTED_TIME, getPrettyQueryDistributedFinishTime());
        executionSummaryProfile.addInfoString(INIT_SCAN_NODE_TIME, getPrettyInitScanNodeTime());
        executionSummaryProfile.addInfoString(FINALIZE_SCAN_NODE_TIME, getPrettyFinalizeScanNodeTime());
        executionSummaryProfile.addInfoString(GET_SPLITS_TIME, getPrettyGetSplitsTime());
        executionSummaryProfile.addInfoString(GET_PARTITIONS_TIME, getPrettyGetPartitionsTime());
        executionSummaryProfile.addInfoString(GET_PARTITION_FILES_TIME, getPrettyGetPartitionFilesTime());
        executionSummaryProfile.addInfoString(CREATE_SCAN_RANGE_TIME, getPrettyCreateScanRangeTime());
        executionSummaryProfile.addInfoString(COMPUTE_ASSIGNMENT_TIME, getPrettyComputeAssignmentTime());
        executionSummaryProfile.addInfoString(COMPUTE_ASSIGNMENT_INFO, getPrettyComputeAssignmentStats());
        executionSummaryProfile.addInfoString(SCHEDULE_TIME, getPrettyQueryScheduleFinishTime());
        executionSummaryProfile.addInfoString(FETCH_RESULT_TIME,
                RuntimeProfile.printCounter(queryFetchResultConsumeTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(WRITE_RESULT_TIME,
                RuntimeProfile.printCounter(queryWriteResultConsumeTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(WAIT_FETCH_RESULT_TIME, getPrettyQueryFetchResultFinishTime());
    }

    public void setNereidsAnalysisTime() {
        this.nereidsAnalysisFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setNereidsRewriteTime() {
        this.nereidsRewriteFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setNereidsOptimizeTime() {
        this.nereidsOptimizeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setNereidsTranslateTime() {
        this.nereidsTranslateFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryBeginTime() {
        this.queryBeginTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryAnalysisFinishTime() {
        this.queryAnalysisFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryJoinReorderFinishTime() {
        this.queryJoinReorderFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setCreateSingleNodeFinishTime() {
        this.queryCreateSingleNodeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setInitScanNodeStartTime() {
        this.initScanNodeStartTime = TimeUtils.getStartTimeMs();
    }

    public void setInitScanNodeFinishTime() {
        this.initScanNodeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setFinalizeScanNodeStartTime() {
        this.finalizeScanNodeStartTime = TimeUtils.getStartTimeMs();
    }

    public void setFinalizeScanNodeFinishTime() {
        this.finalizeScanNodeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setGetSplitsStartTime() {
        this.getSplitsStartTime = TimeUtils.getStartTimeMs();
    }

    public void setGetPartitionsFinishTime() {
        this.getPartitionsFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setGetPartitionFilesFinishTime() {
        this.getPartitionFilesFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setGetSplitsFinishTime() {
        this.getSplitsFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setCreateScanRangeFinishTime() {
        this.createScanRangeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryDistributedFinishTime() {
        this.queryDistributedFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryPlanFinishTime() {
        this.queryPlanFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryScheduleFinishTime() {
        this.queryScheduleFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryFetchResultFinishTime() {
        this.queryFetchResultFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setTempStartTime() {
        this.tempStarTime = TimeUtils.getStartTimeMs();
    }

    public void freshFetchResultConsumeTime() {
        this.queryFetchResultConsumeTime += TimeUtils.getStartTimeMs() - tempStarTime;
    }

    public void freshWriteResultConsumeTime() {
        this.queryWriteResultConsumeTime += TimeUtils.getStartTimeMs() - tempStarTime;
    }

    public void setComputeAssignmentTime() {
        this.computeAssignmentTime = TimeUtils.getStartTimeMs();
    }


    public void setComputeAssignment(Multimap<Backend, Split> assignment) {
        this.assignment = assignment;
    }

    public long getQueryBeginTime() {
        return queryBeginTime;
    }

    public static class SummaryBuilder {
        private Map<String, String> map = Maps.newHashMap();

        public SummaryBuilder profileId(String val) {
            map.put(PROFILE_ID, val);
            return this;
        }

        public SummaryBuilder dorisVersion(String val) {
            map.put(DORIS_VERSION, val);
            return this;
        }

        public SummaryBuilder taskType(String val) {
            map.put(TASK_TYPE, val);
            return this;
        }

        public SummaryBuilder startTime(String val) {
            map.put(START_TIME, val);
            return this;
        }

        public SummaryBuilder endTime(String val) {
            map.put(END_TIME, val);
            return this;
        }

        public SummaryBuilder totalTime(String val) {
            map.put(TOTAL_TIME, val);
            return this;
        }

        public SummaryBuilder taskState(String val) {
            map.put(TASK_STATE, val);
            return this;
        }

        public SummaryBuilder user(String val) {
            map.put(USER, val);
            return this;
        }

        public SummaryBuilder defaultDb(String val) {
            map.put(DEFAULT_DB, val);
            return this;
        }

        public SummaryBuilder workloadGroup(String workloadGroup) {
            map.put(WORKLOAD_GROUP, workloadGroup);
            return this;
        }

        public SummaryBuilder sqlStatement(String val) {
            map.put(SQL_STATEMENT, val);
            return this;
        }

        public SummaryBuilder isCached(String val) {
            map.put(IS_CACHED, val);
            return this;
        }

        public SummaryBuilder totalInstancesNum(String val) {
            map.put(TOTAL_INSTANCES_NUM, val);
            return this;
        }

        public SummaryBuilder instancesNumPerBe(String val) {
            map.put(INSTANCES_NUM_PER_BE, val);
            return this;
        }

        public SummaryBuilder parallelFragmentExecInstance(String val) {
            map.put(PARALLEL_FRAGMENT_EXEC_INSTANCE, val);
            return this;
        }

        public SummaryBuilder traceId(String val) {
            map.put(TRACE_ID, val);
            return this;
        }

        public SummaryBuilder isNereids(String isNereids) {
            map.put(IS_NEREIDS, isNereids);
            return this;
        }

        public SummaryBuilder isPipeline(String isPipeline) {
            map.put(IS_PIPELINE, isPipeline);
            return this;
        }

        public Map<String, String> build() {
            return map;
        }
    }

    private String getPrettyNereidsAnalysisTime() {
        if (nereidsAnalysisFinishTime == -1 || queryAnalysisFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(nereidsAnalysisFinishTime - queryBeginTime, TUnit.TIME_MS);
    }

    private String getPrettyNereidsRewriteTime() {
        if (nereidsRewriteFinishTime == -1 || nereidsAnalysisFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(nereidsRewriteFinishTime - nereidsAnalysisFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyNereidsOptimizeTime() {
        if (nereidsOptimizeFinishTime == -1 || nereidsRewriteFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(nereidsOptimizeFinishTime - nereidsRewriteFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyNereidsTranslateTime() {
        if (nereidsTranslateFinishTime == -1 || nereidsOptimizeFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(nereidsTranslateFinishTime - nereidsOptimizeFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyQueryAnalysisFinishTime() {
        if (queryBeginTime == -1 || queryAnalysisFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryAnalysisFinishTime - queryBeginTime, TUnit.TIME_MS);
    }

    private String getPrettyQueryJoinReorderFinishTime() {
        if (queryAnalysisFinishTime == -1 || queryJoinReorderFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryJoinReorderFinishTime - queryAnalysisFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyCreateSingleNodeFinishTime() {
        if (queryJoinReorderFinishTime == -1 || queryCreateSingleNodeFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryCreateSingleNodeFinishTime - queryJoinReorderFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyQueryDistributedFinishTime() {
        if (queryCreateSingleNodeFinishTime == -1 || queryDistributedFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryDistributedFinishTime - queryCreateSingleNodeFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyInitScanNodeTime() {
        if (initScanNodeStartTime == -1 || initScanNodeFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(initScanNodeFinishTime - initScanNodeStartTime, TUnit.TIME_MS);
    }

    private String getPrettyFinalizeScanNodeTime() {
        if (finalizeScanNodeFinishTime == -1 || finalizeScanNodeStartTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(finalizeScanNodeFinishTime - finalizeScanNodeStartTime, TUnit.TIME_MS);
    }

    private String getPrettyGetSplitsTime() {
        if (getSplitsFinishTime == -1 || getSplitsStartTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(getSplitsFinishTime - getSplitsStartTime, TUnit.TIME_MS);
    }

    private String getPrettyGetPartitionsTime() {
        if (getSplitsStartTime == -1 || getPartitionsFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(getPartitionsFinishTime - getSplitsStartTime, TUnit.TIME_MS);
    }

    private String getPrettyGetPartitionFilesTime() {
        if (getPartitionsFinishTime == -1 || getPartitionFilesFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(getPartitionFilesFinishTime - getPartitionsFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyCreateScanRangeTime() {
        if (getSplitsFinishTime == -1 || createScanRangeFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(createScanRangeFinishTime - getSplitsFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyQueryPlanFinishTime() {
        if (queryAnalysisFinishTime == -1 || queryPlanFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryPlanFinishTime - queryAnalysisFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyQueryScheduleFinishTime() {
        if (queryPlanFinishTime == -1 || queryScheduleFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryScheduleFinishTime - queryPlanFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyQueryFetchResultFinishTime() {
        if (queryScheduleFinishTime == -1 || queryFetchResultFinishTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(queryFetchResultFinishTime - queryScheduleFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyComputeAssignmentTime() {
        if (getSplitsFinishTime == -1 || computeAssignmentTime == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(computeAssignmentTime - getSplitsFinishTime, TUnit.TIME_MS);
    }

    private String getPrettyComputeAssignmentStats() {
        List<Integer> loads = new ArrayList<>(assignment.keySet().size());
        List<Long> splitBytes = new ArrayList<>(assignment.keySet().size());
        int totalSplitSize = 0;
        long totalSplitBytes = 0;
        for (Backend backend : assignment.keySet()) {
            Collection<Split> splits = assignment.get(backend);
            loads.add(splits.size());
            totalSplitSize += splits.size();
            totalSplitBytes += splits.stream()
                        .mapToLong(x -> x.getLength())
                        .sum();
        }
        float avg = totalSplitSize * 1.0f / loads.size();
        Collections.sort(loads);
        double t = 0.0;
        for (Integer x : loads) {
            t += (x - avg) * (x - avg);
        }
        t = Math.sqrt(t / loads.size());

        System.out.printf("Load: min = %d, max = %d, median = %d, stddev = %.2f\n", loads.get(0),
                loads.get(loads.size() - 1), loads.get(loads.size() / 2), t);

        float splitByteAVG = totalSplitBytes * 1.0f / splitBytes.size();
        Collections.sort(splitBytes);
        double splitBytesStdDev = 0.0;
        for (Long x : splitBytes) {
            splitBytesStdDev += (x - splitByteAVG) * (x - splitByteAVG);
        }
        splitBytesStdDev = Math.sqrt(splitBytesStdDev / splitBytes.size());

        System.out.printf("Load: min = %d, max = %d, median = %d, stddev = %.2f\n", splitBytes.get(0),
                loads.get(splitBytes.size() - 1), loads.get(splitBytes.size() / 2), splitBytesStdDev);

        return String.format("NodeSelectionStrategy: %s; Split num: Min = %d, Max = %d, Median = %d, StdDev = %.2f; Split num: Min = %d, Max = %d, Median = %d, StdDev = %.2f;", loads.get(0),
                loads.get(loads.size() - 1), loads.get(loads.size() / 2), t, splitBytes.get(0),
                loads.get(splitBytes.size() - 1), loads.get(splitBytes.size() / 2), splitBytesStdDev);
    }
}
