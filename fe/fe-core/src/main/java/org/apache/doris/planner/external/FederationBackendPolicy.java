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

package org.apache.doris.planner.external;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import static java.util.Comparator.comparingLong;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FederationBackendPolicy {
    private static final Logger LOG = LogManager.getLogger(FederationBackendPolicy.class);
    private final List<Backend> backends = Lists.newArrayList();
    private final Map<String, List<Backend>> backendMap = Maps.newHashMap();
    private List<TScanRangeLocations> scanRangeLocationsList;

    public Map<Backend, Long> getAssignedScanBytesPerBackend() {
        return assignedScanBytesPerBackend;
    }

    private Map<Backend, Long> assignedWeightPerBackend = Maps.newHashMap();
    private Map<Backend, Long> assignedScanBytesPerBackend = Maps.newHashMap();
    private final SecureRandom random = new SecureRandom();
    // private ConsistentHash<TScanRangeLocations, Backend> consistentHash;

    private HashRing<TScanRangeLocations, Backend> hashRing;


    private int nextBe = 0;
    private boolean initialized = false;

    private long avgNodeScanRangeBytes;

    private long maxImbalanceBytes;

    private long avgScanRangeBytes;

    // Create a ConsistentHash ring may be a time-consuming operation, so we cache it.
    // private static LoadingCache<HashCacheKey, ConsistentHash<TScanRangeLocations, Backend>> consistentHashCache;
    //
    // static {
    //     consistentHashCache = CacheBuilder.newBuilder().maximumSize(5)
    //             .build(new CacheLoader<HashCacheKey, ConsistentHash<TScanRangeLocations, Backend>>() {
    //                 @Override
    //                 public ConsistentHash<TScanRangeLocations, Backend> load(HashCacheKey key) {
    //                     return new ConsistentHash<>(Hashing.murmur3_128(), new ScanRangeHash(),
    //                             new BackendHash(), key.bes, Config.virtual_node_number);
    //                 }
    //             });
    // }

    private static LoadingCache<HashCacheKey, HashRing<TScanRangeLocations, Backend>> hashRingCache;

    static {
        hashRingCache = CacheBuilder.newBuilder().maximumSize(5)
                .build(new CacheLoader<HashCacheKey, HashRing<TScanRangeLocations, Backend>>() {
                    @Override
                    public HashRing<TScanRangeLocations, Backend> load(HashCacheKey key) {
                        return new ConsistentHashRing<>(Hashing.murmur3_128(), new ScanRangeHash(),
                                new BackendHash(), key.bes, 2048);
                    }
                });
    }

    private static class HashCacheKey {
        // sorted backend ids as key
        private List<Long> beIds;
        // backends is not part of key, just an attachment
        private List<Backend> bes;

        HashCacheKey(List<Backend> backends) {
            this.bes = backends;
            this.beIds = backends.stream().map(b -> b.getId()).sorted().collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof HashCacheKey)) {
                return false;
            }
            return Objects.equals(beIds, ((HashCacheKey) obj).beIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(beIds);
        }

        @Override
        public String toString() {
            return "HashCache{" + "beIds=" + beIds + '}';
        }
    }

    public void init() throws UserException {
        if (!initialized) {
            init(Collections.emptyList());
            initialized = true;
        }
    }

    public void init(List<String> preLocations) throws UserException {
        Set<Tag> tags = Sets.newHashSet();
        if (ConnectContext.get() != null && ConnectContext.get().getCurrentUserIdentity() != null) {
            String qualifiedUser = ConnectContext.get().getCurrentUserIdentity().getQualifiedUser();
            // Some request from stream load(eg, mysql load) may not set user info in ConnectContext
            // just ignore it.
            if (!Strings.isNullOrEmpty(qualifiedUser)) {
                tags = Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser);
                if (tags == UserProperty.INVALID_RESOURCE_TAGS) {
                    throw new UserException("No valid resource tag for user: " + qualifiedUser);
                }
            }
        } else {
            LOG.debug("user info in ExternalFileScanNode should not be null, add log to observer");
        }

        // scan node is used for query
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .needLoadAvailable()
                .addTags(tags)
                .preferComputeNode(Config.prefer_compute_node_for_external_table)
                .assignExpectBeNum(Config.min_backend_num_for_external_table)
                .addPreLocations(preLocations)
                .build();
        init(policy);
    }

    public void init(BeSelectionPolicy policy) throws UserException {
        backends.addAll(policy.getCandidateBackends(Env.getCurrentSystemInfo().getIdToBackend().values()));
        if (backends.isEmpty()) {
            throw new UserException("No available backends");
        }
        for (Backend backend : backends) {
            assignedScanBytesPerBackend.put(backend, 0L);
        }

        backendMap.putAll(backends.stream().collect(Collectors.groupingBy(Backend::getHost)));
        try {
            // consistentHash = consistentHashCache.get(new HashCacheKey(backends));
            hashRing = hashRingCache.get(new HashCacheKey(backends));
        } catch (ExecutionException e) {
            throw new UserException("failed to get consistent hash", e);
        }

    }

    public void setScanRangeLocationsList(List<TScanRangeLocations> scanRangeLocationsList) {
        // long totalSize = tScanRangeLocationsList.stream().mapToLong(x->x.scan_range.ext_scan_range.file_scan_range.ranges.stream().mapToLong(y -> y.size).sum())
        //         .sum();
        this.scanRangeLocationsList = scanRangeLocationsList;
        long totalSize = 0;
        int totalRangeNum = 0;
        for (TScanRangeLocations scanRangeLocations : scanRangeLocationsList) {
            for (TFileRangeDesc range : scanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges) {
                totalSize += range.size;
                ++totalRangeNum;
            }
        }
        avgNodeScanRangeBytes = totalSize / scanRangeLocationsList.size() + 1;
        // avgNodeScanRangeBytes = totalSize / Math.max(backends.size(), 1) + 1;
        avgNodeScanRangeBytes = totalSize / Math.max(backends.size(), 1) + 1;
        maxImbalanceBytes = avgNodeScanRangeBytes * 3;
        // System.out.println("avgScanRangeBytes: " + avgNodeScanRangeBytes + ", maxImbalanceBytes: " + maxImbalanceBytes);
        avgScanRangeBytes = (totalRangeNum > 0) ? (long) Math.ceil((double) totalSize / totalRangeNum) : 0L;

    }

    public Backend getNextBe() {
        Backend selectedBackend = backends.get(nextBe++);
        nextBe = nextBe % backends.size();
        return selectedBackend;
    }

    public Backend getNextConsistentBe(TScanRangeLocations scanRangeLocations) {
        // return consistentHash.getNode(scanRangeLocations);
        List<Backend> backends = hashRing.get(scanRangeLocations, 3);
        // Backend backend = reBalanceScanRangeForComputeNode(backends, avgNodeScanRangeBytes, scanRangeLocations);
        Backend backend = reBalanceScanRangeForComputeNode(backends, maxImbalanceBytes);
        if (backend == null) {
            throw new RuntimeException("Failed to find backend to execute");
        }
        recordScanRangeAssignment(backend, backends, scanRangeLocations);
        return backend;
    }

    public static class ScanRangeLocationsAndSplit {
        private TScanRangeLocations scanRangeLocations;
        private Split split;

        public ScanRangeLocationsAndSplit(TScanRangeLocations scanRangeLocations, Split split) {
            this.scanRangeLocations = scanRangeLocations;
            this.split = split;
        }

        public TScanRangeLocations getScanRangeLocations() {
            return scanRangeLocations;
        }

        public Split getSplit() {
            return split;
        }
    }

    public Multimap<Backend, ScanRangeLocationsAndSplit> computeScanRangeAssignment(List<Split> splits) throws UserException {
        // Multimap<Backend, ScanRangeLocationsAndSplit> assignment = HashMultimap.create();
        ListMultimap<Backend, ScanRangeLocationsAndSplit> assignment = ArrayListMultimap.create();

        List<ScanRangeLocationsAndSplit> remainingScanRangeLocationsAndSplits = new ArrayList<>(splits.size());

        List<Backend> filteredNodes = new ArrayList<>();
        for (List<Backend> backendList : backendMap.values()) {
            filteredNodes.addAll(backendList);
        }
        ResettableRandomizedIterator<Backend> randomCandidates = new ResettableRandomizedIterator<>(filteredNodes);
        Set<Backend> schedulableNodes = new HashSet<>(filteredNodes);

        boolean splitsToBeRedistributed = false;
        boolean optimizedLocalScheduling = true;
        int minCandidates = 10;

        // optimizedLocalScheduling enables prioritized assignment of splits to local nodes when splits contain locality information
        if (optimizedLocalScheduling) {
            for (int i = 0; i < scanRangeLocationsList.size(); ++i) {
                TScanRangeLocations scanRangeLocations = scanRangeLocationsList.get(i);
                Split split = splits.get(i);
                ScanRangeLocationsAndSplit scanRangeLocationsAndSplit = new ScanRangeLocationsAndSplit(scanRangeLocations, split);
                if (split.isRemotelyAccessible() && (split.getHosts() != null && split.getHosts().length > 0)) {
                    List<Backend> candidateNodes = selectExactNodes(backendMap, split.getHosts());

                    Optional<Backend> chosenNode = candidateNodes.stream()
                            // .filter(ownerNode -> assignmentStats.getTotalSplitsWeight(ownerNode) < maxSplitsWeightPerNode && assignmentStats.getUnacknowledgedSplitCountForStage(ownerNode) < maxUnacknowledgedSplitsPerTask)
                            .min(comparingLong(ownerNode -> assignedScanBytesPerBackend.get(ownerNode)));

                    if (chosenNode.isPresent()) {
                        Backend selectedBackend = chosenNode.get();
                        assignment.put(selectedBackend, scanRangeLocationsAndSplit);
                        long addedScans = scanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges.stream()
                                .mapToLong(x -> x.size)
                                .sum();
                        assignedScanBytesPerBackend.put(selectedBackend,
                                assignedScanBytesPerBackend.get(selectedBackend) + addedScans);
                        splitsToBeRedistributed = true;
                        continue;
                    }
                }
                remainingScanRangeLocationsAndSplits.add(new ScanRangeLocationsAndSplit(scanRangeLocations, split));
            }
        } else {
            for (int i = 0; i < scanRangeLocationsList.size(); ++i) {
                TScanRangeLocations scanRangeLocations = scanRangeLocationsList.get(i);
                Split split = splits.get(i);
                remainingScanRangeLocationsAndSplits.add(new ScanRangeLocationsAndSplit(scanRangeLocations, split));
            }
        }

        for (ScanRangeLocationsAndSplit scanRangeLocationsAndSplit : remainingScanRangeLocationsAndSplits) {
            TScanRangeLocations scanRangeLocations = scanRangeLocationsAndSplit.getScanRangeLocations();
            Split split = scanRangeLocationsAndSplit.getSplit();
            randomCandidates.reset();

            List<Backend> candidateNodes;
            if (!split.isRemotelyAccessible()) {
                candidateNodes = selectExactNodes(backendMap, split.getHosts());
            } else {
                // candidateNodes = selectNodes(minCandidates, randomCandidates);
                candidateNodes = hashRing.get(scanRangeLocations, 3);
            }
            if (candidateNodes.isEmpty()) {
                // log.debug("No nodes available to schedule %s. Available nodes %s", split,
                //         nodeMap.getNodesByHost().keys());
                // throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            Backend selectedBackend = chooseNodeForSplit(candidateNodes);
            if (selectedBackend != null) {
                assignment.put(selectedBackend, scanRangeLocationsAndSplit);
                long addedScans = scanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges.stream()
                        .mapToLong(x -> x.size)
                        .sum();
                assignedScanBytesPerBackend.put(selectedBackend,
                        assignedScanBytesPerBackend.get(selectedBackend) + addedScans);
            } else {
                // candidateNodes.forEach(schedulableNodes::remove);
                // if (split.isRemotelyAccessible()) {
                //     splitWaitingForAnyNode = true;
                // }
                // // Exact node set won't matter, if a split is waiting for any node
                // else if (!splitWaitingForAnyNode) {
                //     blockedExactNodes.addAll(candidateNodes);
                // }
                //
                // if (splitWaitingForAnyNode && schedulableNodes.isEmpty()) {
                //     // All nodes assigned, no need to test if we can assign new split
                //     break;
                // }
            }
        }

        // ListenableFuture<Void> blocked;
        // if (splitWaitingForAnyNode) {
        //     blocked = toWhenHasSplitQueueSpaceFuture(existingTasks,
        //             calculateLowWatermark(minPendingSplitsWeightPerTask));
        // } else {
        //     blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks,
        //             calculateLowWatermark(minPendingSplitsWeightPerTask));
        // }

        // if (splitsToBeRedistributed) {
            equateDistribution(assignment);
        // }
        return assignment;
    }

    /**
     * The method tries to make the distribution of splits more uniform. All nodes are arranged into a maxHeap and a minHeap
     * based on the number of splits that are assigned to them. Splits are redistributed, one at a time, from a maxNode to a
     * minNode until we have as uniform a distribution as possible.
     *
     * @param assignment the node-splits multimap after the first and the second stage
     */
    private void equateDistribution(ListMultimap<Backend, ScanRangeLocationsAndSplit> assignment) {
        if (assignment.isEmpty()) {
            return;
        }

        List<Backend> allNodes = new ArrayList<>();
        for (List<Backend> backendList : backendMap.values()) {
            allNodes.addAll(backendList);
        }

        if (allNodes.size() < 2) {
            return;
        }

        IndexedPriorityQueue<Backend> maxNodes = new IndexedPriorityQueue<>();
        for (Backend node : assignment.keySet()) {
            maxNodes.addOrUpdate(node, assignedScanBytesPerBackend.get(node));
        }

        IndexedPriorityQueue<Backend> minNodes = new IndexedPriorityQueue<>();
        for (Backend node : allNodes) {
            minNodes.addOrUpdate(node, Long.MAX_VALUE - assignedScanBytesPerBackend.get(node));
        }

        while (true) {
            if (maxNodes.isEmpty()) {
                return;
            }

            // fetch min and max node
            Backend maxNode = maxNodes.poll();
            Backend minNode = minNodes.poll();

            // Allow some degree of non uniformity when assigning splits to nodes. Usually data distribution
            // among nodes in a cluster won't be fully uniform (e.g. because hash function with non-uniform
            // distribution is used like consistent hashing). In such case it makes sense to assign splits to nodes
            // with data because of potential savings in network throughput and CPU time.
            // The difference of 5 between node with maximum and minimum splits is a tradeoff between ratio of
            // misassigned splits and assignment uniformity. Using larger numbers doesn't reduce the number of
            // misassigned splits greatly (in absolute values).
            // if (assignedScanBytesPerBackend.get(maxNode) - assignedScanBytesPerBackend.get(minNode)
            //         <= SplitWeight.rawValueForStandardSplitCount(5)) {
            //     return;
            // }
            if (assignedScanBytesPerBackend.get(maxNode) - assignedScanBytesPerBackend.get(minNode)
                    <= avgScanRangeBytes * 5) {
                return;
            }

            // move split from max to min
            ScanRangeLocationsAndSplit redistributed = redistributeSplit(assignment, maxNode, minNode);
            // assignmentStats.removeAssignedSplit(maxNode, redistributed.getSplitWeight());
            // assignmentStats.addAssignedSplit(minNode, redistributed.getSplitWeight());

            long scanBytes = redistributed.scanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges.stream()
                    .mapToLong(x -> x.size)
                    .sum();
            // assignedScanBytesPerBackend.put(maxNode, assignedScanBytesPerBackend.get(maxNode) - redistributed.getSplit().getSplitWeight().getRawValue());
            // assignedScanBytesPerBackend.put(minNode, assignedScanBytesPerBackend.get(maxNode) + redistributed.getSplit().getSplitWeight().getRawValue());


            assignedScanBytesPerBackend.put(maxNode, assignedScanBytesPerBackend.get(maxNode) - scanBytes);
            assignedScanBytesPerBackend.put(minNode, assignedScanBytesPerBackend.get(maxNode) + scanBytes);

            // add max back into maxNodes only if it still has assignments
            if (assignment.containsKey(maxNode)) {
                maxNodes.addOrUpdate(maxNode, assignedScanBytesPerBackend.get(maxNode));
            }

            // Add or update both the Priority Queues with the updated node priorities
            maxNodes.addOrUpdate(minNode, assignedScanBytesPerBackend.get(minNode));
            minNodes.addOrUpdate(minNode, Long.MAX_VALUE - assignedScanBytesPerBackend.get(minNode));
            minNodes.addOrUpdate(maxNode, Long.MAX_VALUE - assignedScanBytesPerBackend.get(maxNode));
        }
    }

    /**
     * The method selects and removes a split from the fromNode and assigns it to the toNode. There is an attempt to
     * redistribute a Non-local split if possible. This case is possible when there are multiple queries running
     * simultaneously. If a Non-local split cannot be found in the maxNode, next split is selected and reassigned.
     */
    @VisibleForTesting
    public static ScanRangeLocationsAndSplit redistributeSplit(Multimap<Backend, ScanRangeLocationsAndSplit> assignment, Backend fromNode,
            Backend toNode) {
        Iterator<ScanRangeLocationsAndSplit> scanRangeLocationsAndSplitIterator = assignment.get(fromNode).iterator();
        ScanRangeLocationsAndSplit splitToBeRedistributed = null;
        while (scanRangeLocationsAndSplitIterator.hasNext()) {
            ScanRangeLocationsAndSplit scanRangeLocationsAndSplit = scanRangeLocationsAndSplitIterator.next();
            // Try to select non-local split for redistribution
            if (splitToBeRedistributed.split.getHosts() != null && !isSplitLocal(
                    splitToBeRedistributed.split.getHosts(), fromNode.getHost())) {
                splitToBeRedistributed = scanRangeLocationsAndSplit;
                break;
            }
        }
        // Select split if maxNode has no non-local splits in the current batch of assignment
        if (splitToBeRedistributed == null) {
            scanRangeLocationsAndSplitIterator = assignment.get(fromNode).iterator();
            while (scanRangeLocationsAndSplitIterator.hasNext()) {
                splitToBeRedistributed = scanRangeLocationsAndSplitIterator.next();
                // if toNode has split replication, transfer this split firstly
                if (splitToBeRedistributed.split.getHosts() != null && isSplitLocal(
                        splitToBeRedistributed.split.getHosts(), toNode.getHost())) {
                    break;
                }
            }
        }
        scanRangeLocationsAndSplitIterator.remove();
        assignment.put(toNode, splitToBeRedistributed);
        return splitToBeRedistributed;
    }

    private static boolean isSplitLocal(String[] splitHosts, String host) {

        for (String splitHost : splitHosts) {
            if (splitHost.equals(host)) {
                return true;
            }
            // InetAddress inetAddress;
            // try {
            //     inetAddress = address.toInetAddress();
            // }
            // catch (UnknownHostException e) {
            //     continue;
            // }
            // if (!address.hasPort()) {
            //     Set<Backend> localNodes = nodesByHost.get(inetAddress);
            //     return localNodes.stream()
            //             .anyMatch(node -> node.getHostAndPort().equals(nodeAddress));
            // }
        }
        return false;
    }

    /**
     * Helper method to determine if a split is local to a node irrespective of whether splitAddresses contain port information or not
     */
    // private static boolean isSplitLocal(List<HostAddress> splitAddresses, HostAddress nodeAddress, SetMultimap<InetAddress, Backend> nodesByHost)
    // {
    //     for (HostAddress address : splitAddresses) {
    //         if (nodeAddress.equals(address)) {
    //             return true;
    //         }
    //         InetAddress inetAddress;
    //         try {
    //             inetAddress = address.toInetAddress();
    //         }
    //         catch (UnknownHostException e) {
    //             continue;
    //         }
    //         if (!address.hasPort()) {
    //             Set<Backend> localNodes = nodesByHost.get(inetAddress);
    //             return localNodes.stream()
    //                     .anyMatch(node -> node.getHostAndPort().equals(nodeAddress));
    //         }
    //     }
    //     return false;
    // }
    public static List<Backend> selectExactNodes(Map<String, List<Backend>> backendMap, String[] hosts) {
        Set<Backend> chosen = new LinkedHashSet<>();

        for (String host : hosts) {
            backendMap.get(host).stream()
                    .forEach(chosen::add);

            //     // consider a split with a host without a port as being accessible by all nodes in that host
            //     if (!host.hasPort()) {
            //         InetAddress address;
            //         try {
            //             address = host.toInetAddress();
            //         }
            //         catch (UnknownHostException e) {
            //             // skip hosts that don't resolve
            //             continue;
            //         }
            //
            //         nodeMap.getNodesByHost().get(address).stream()
            //                 .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
            //                 .forEach(chosen::add);
            //     }
        }

        // // if the chosen set is empty and the host is the coordinator, force pick the coordinator
        // if (chosen.isEmpty() && !includeCoordinator) {
        //     for (HostAddress host : hosts) {
        //         // In the code below, before calling `chosen::add`, it could have been checked that
        //         // `coordinatorIds.contains(node.getNodeIdentifier())`. But checking the condition isn't necessary
        //         // because every node satisfies it. Otherwise, `chosen` wouldn't have been empty.
        //
        //         chosen.addAll(nodeMap.getNodesByHostAndPort().get(host));
        //
        //         // consider a split with a host without a port as being accessible by all nodes in that host
        //         if (!host.hasPort()) {
        //             InetAddress address;
        //             try {
        //                 address = host.toInetAddress();
        //             }
        //             catch (UnknownHostException e) {
        //                 // skip hosts that don't resolve
        //                 continue;
        //             }
        //
        //             chosen.addAll(nodeMap.getNodesByHost().get(address));
        //         }
        //     }
        // }

        return ImmutableList.copyOf(chosen);
    }

    public static List<Backend> selectNodes(int limit, Iterator<Backend> candidates) {
        checkArgument(limit > 0, "limit must be at least 1");

        List<Backend> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            selected.add(candidates.next());
        }

        return selected;
    }

    private Backend chooseNodeForSplit(List<Backend> candidateNodes) {
        Backend chosenNode = null;
        long minWeight = Long.MAX_VALUE;

        for (Backend node : candidateNodes) {
            long queuedWeight = assignedScanBytesPerBackend.get(node);
            if (queuedWeight <= minWeight) {
                chosenNode = node;
                minWeight = queuedWeight;
            }
        }

        return chosenNode;
    }

    // private Backend reBalanceScanRangeForComputeNode(List<Backend> backends, long avgNodeScanRangeBytes,
    //         TScanRangeLocations scanRangeLocations) {
    //     if (backends == null || backends.isEmpty()) {
    //         return null;
    //     }
    //
    //     Backend res = null;
    //     long addedScans = scanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges.stream().mapToLong(x -> x.size)
    //             .sum();
    //     for (Backend backend : backends) {
    //         long assignedScanRanges = assignedScansPerComputeNode.get(backend);
    //         if (assignedScanRanges + addedScans < avgNodeScanRangeBytes * 1.1) {
    //             res = backend;
    //             break;
    //         }
    //     }
    //     if (res == null) {
    //         res = backends.get(0);
    //     }
    //     return res;
    // }

    private Backend reBalanceScanRangeForComputeNode(List<Backend> backends, long maxImbalanceBytes) {
        if (backends == null || backends.isEmpty()) {
            return null;
        }

        Backend node = null;
        long minAssignedScanRanges = Long.MAX_VALUE;
        for (Backend backend : backends) {
            long assignedScanRanges = assignedScanBytesPerBackend.get(backend);
            if (assignedScanRanges < minAssignedScanRanges) {
                minAssignedScanRanges = assignedScanRanges;
                node = backend;
            }
        }
        if (maxImbalanceBytes == 0) {
            return node;
        }

        for (Backend backend : backends) {
            long assignedScanRanges = assignedScanBytesPerBackend.get(backend);
            if (assignedScanRanges < (minAssignedScanRanges + maxImbalanceBytes)) {
                node = backend;
                break;
            }
        }
        return node;
    }

    private void recordScanRangeAssignment(Backend worker, List<Backend> backends,
            TScanRangeLocations scanRangeLocations) {
        // workerProvider.selectWorker(worker.getId());

        // update statistic
        long addedScans = scanRangeLocations.scan_range.ext_scan_range.file_scan_range.ranges.stream()
                .mapToLong(x -> x.size)
                .sum();
        // System.out.println("addedScans: " + addedScans);
        assignedScanBytesPerBackend.put(worker, assignedScanBytesPerBackend.get(worker) + addedScans);
        // // the fist item in backends will be assigned if there is no re-balance, we compute re-balance bytes
        // // if the worker is not the first item in backends.
        // if (worker != backends.get(0)) {
        //     reBalanceBytesPerComputeNode.put(worker, reBalanceBytesPerComputeNode.get(worker) + addedScans);
        // }
        //
        // // add scan range params
        // TScanRangeParams scanRangeParams = new TScanRangeParams();
        // scanRangeParams.scan_range = scanRangeLocations.scan_range;
        // assignment.put(worker.getId(), scanNode.getId().asInt(), scanRangeParams);
    }

    // Try to find a local BE, if not exists, use `getNextBe` instead
    public Backend getNextLocalBe(List<String> hosts, TScanRangeLocations scanRangeLocations) {
        List<Backend> candidateBackends = Lists.newArrayListWithCapacity(hosts.size());
        for (String host : hosts) {
            List<Backend> backends = backendMap.get(host);
            if (CollectionUtils.isNotEmpty(backends)) {
                candidateBackends.add(backends.get(random.nextInt(backends.size())));
            }
        }

        return CollectionUtils.isEmpty(candidateBackends)
                ? getNextConsistentBe(scanRangeLocations)
                : candidateBackends.get(random.nextInt(candidateBackends.size()));
    }

    public int numBackends() {
        return backends.size();
    }

    public Collection<Backend> getBackends() {
        return CollectionUtils.unmodifiableCollection(backends);
    }

    private static class BackendHash implements Funnel<Backend> {
        @Override
        public void funnel(Backend backend, PrimitiveSink primitiveSink) {
            primitiveSink.putLong(backend.getId());
            // primitiveSink.putString(backend.getHost(), StandardCharsets.UTF_8);
            // primitiveSink.putInt(backend.getBePort());
        }
    }

    private static class ScanRangeHash implements Funnel<TScanRangeLocations> {
        @Override
        public void funnel(TScanRangeLocations scanRange, PrimitiveSink primitiveSink) {
            Preconditions.checkState(scanRange.scan_range.isSetExtScanRange());
            for (TFileRangeDesc desc : scanRange.scan_range.ext_scan_range.file_scan_range.ranges) {
                primitiveSink.putBytes(desc.path.getBytes(StandardCharsets.UTF_8));
                primitiveSink.putLong(desc.start_offset);
                primitiveSink.putLong(desc.size);

                // primitiveSink.putString(desc.path, StandardCharsets.UTF_8);
                // primitiveSink.putLong(desc.start_offset);
            }
        }
    }
}
