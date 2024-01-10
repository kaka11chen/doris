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

package org.apache.doris.planner;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.external.FederationBackendPolicy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Stopwatch;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FederationBackendPolicyTest {
    @Mocked
    private Env env;

    @Before
    public void setUp() {

        SystemInfoService service = new SystemInfoService();

        // for (int i = 0; i < 190; i++) {
        //     Backend backend = new Backend(Long.valueOf(i), "192.168.1." + i, 9050);
        //     backend.setAlive(true);
        //     service.addBackend(backend);
        // }
        // for (int i = 0; i < 10; i++) {
        //     Backend backend = new Backend(Long.valueOf(190 + i), "192.168.1." + i, 9051);
        //     backend.setAlive(true);
        //     service.addBackend(backend);
        // }
        // for (int i = 0; i < 10; i++) {
        //     Backend backend = new Backend(Long.valueOf(200 + i), "192.168.2." + i, 9050);
        //     backend.setAlive(false);
        //     service.addBackend(backend);
        // }

        // Backend backend1 = new Backend(10010L, "172.30.0.100", 29052);
        // backend1.setAlive(true);
        // service.addBackend(backend1);
        // Backend backend2 = new Backend(10562L, "172.30.0.106", 29052);
        // backend2.setAlive(true);
        // service.addBackend(backend2);
        // Backend backend3 = new Backend(10563L, "172.30.0.118", 29052);
        // backend3.setAlive(true);
        // service.addBackend(backend3);

        Backend backend1 = new Backend(10002L, "172.30.0.100", 9050);
        backend1.setAlive(true);
        service.addBackend(backend1);
        Backend backend2 = new Backend(10003L, "172.30.0.106", 9050);
        backend2.setAlive(true);
        service.addBackend(backend2);
        Backend backend3 = new Backend(10004L, "172.30.0.118", 9050);
        backend3.setAlive(true);
        service.addBackend(backend3);

        // Backend backend1 = new Backend(10049, "172.21.0.35 ", 29052);
        // backend1.setAlive(true);
        // service.addBackend(backend1);
        // Backend backend2 = new Backend(11001, "172.21.0.42", 29052);
        // backend2.setAlive(true);
        // service.addBackend(backend2);
        // Backend backend3 = new Backend(11002, "172.21.0.18", 29052);
        // backend3.setAlive(true);
        // service.addBackend(backend3);

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

    }

    @Test
    public void testGetNextBe() throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        int backendNum = 200;
        int invokeTimes = 1000000;
        Assertions.assertEquals(policy.numBackends(), backendNum);
        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < invokeTimes; i++) {
            Assertions.assertFalse(policy.getNextBe().getHost().contains("192.168.2."));
        }
        sw.stop();
        System.out.println("Invoke getNextBe() " + invokeTimes
                + " times cost [" + sw.elapsed(TimeUnit.MILLISECONDS) + "] ms");
    }

    @Test
    public void testGetNextLocalBe() throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        int backendNum = 200;
        int invokeTimes = 1000000;
        Assertions.assertEquals(policy.numBackends(), backendNum);
        List<String> localHosts = Arrays.asList("192.168.1.0", "192.168.1.1", "192.168.1.2");
        TScanRangeLocations scanRangeLocations = getScanRangeLocations("path1", 0, 100);
        Stopwatch sw = Stopwatch.createStarted();
        for (int i = 0; i < invokeTimes; i++) {
            Assertions.assertTrue(localHosts.contains(policy.getNextLocalBe(localHosts, scanRangeLocations).getHost()));
        }
        sw.stop();
        System.out.println("Invoke getNextLocalBe() " + invokeTimes
                + " times cost [" + sw.elapsed(TimeUnit.MILLISECONDS) + "] ms");
    }

    @Test
    public void testConsistentHash() throws UserException {
        List<TScanRangeLocations> tScanRangeLocationsList = new ArrayList<>();

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 112140970);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }
        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00001-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 120839661);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00002-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 108897409);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00003-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95795997);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00004-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 104600402);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00005-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 105664025);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00006-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 103711014);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00007-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 89839109);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00008-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 92496155);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00009-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95486297);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00010-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 97797209);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00011-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 92999575);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00012-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 99533306);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00013-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 90694038);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00014-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 97698584);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00015-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 93711208);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00016-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 96421980);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00017-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 85527844);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00018-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 93816383);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00019-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 85108822);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00020-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95133703);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00021-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 97285292);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00022-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 100579259);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00023-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 116336426);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00024-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 116148926);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00025-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 116505199);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00026-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 115584854);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00027-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 115267045);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00028-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 115075866);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00029-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 102530112);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00030-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 87211812);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00031-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 103579671);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00032-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 117549465);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00033-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95350272);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00034-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 129209202);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00035-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 99541126);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00036-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95596606);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00037-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 98935955);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00038-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 98053997);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00039-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 114076340);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00040-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 100283827);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00041-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 110319611);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00042-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 101494421);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00043-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 90715051);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00044-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95438609);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00045-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 89271376);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00046-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 91558685);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00047-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 114013106);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00048-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 105660160);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00049-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 110249221);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00050-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 99168752);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00051-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 94812631);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00052-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 101518254);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00053-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 92684134);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00054-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 115594289);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00055-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 117147337);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00056-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 103054212);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00057-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 99133049);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00058-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 113477713);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00059-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 105573676);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00060-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 101289119);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00061-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 103000549);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00062-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 130999464);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00063-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 99339325);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00064-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95187681);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00065-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 90133725);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00066-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 81722295);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00067-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 133200467);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00068-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 81173340);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00069-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 97476101);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00070-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 123019695);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00071-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 82483037);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00072-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 89934700);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00073-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 85915293);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00074-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 79346323);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00075-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 91137739);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00076-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 90098330);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00077-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 86097570);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00078-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 107224214);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00079-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 95246419);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00080-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 96813113);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00081-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 97914463);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00082-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 75505898);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00083-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 78706217);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00084-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 85443817);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00085-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 76589909);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00086-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 80467286);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00087-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 70001613);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00088-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 81959932);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00089-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 105329976);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00090-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 78376112);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00091-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 68536185);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00092-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 90788360);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00093-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 89103797);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00094-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 75535044);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00095-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 80447661);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00096-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 64534485);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00097-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 71021217);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00098-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 67884371);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00099-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 67534105);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00100-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 77146097);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00101-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 77623424);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00102-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 64304166);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00103-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 56240361);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00104-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 62365536);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00105-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 62458240);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00106-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 65575634);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00107-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 63059469);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00108-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 66047016);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00109-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 43982163);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00110-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 46593529);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00111-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 46353243);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00112-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 102636968);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00113-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 96187364);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00114-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 86263865);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00115-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 80489839);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00116-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 71236762);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00117-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 56855502);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00118-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 83043345);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00119-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 74974787);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00120-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 73602701);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00121-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 74198445);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00122-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 85072457);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00123-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 71091535);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00124-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 60804139);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00125-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 67551643);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00126-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 79319227);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00127-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 62913171);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }

        {
            TScanRangeLocations scanRangeLocations = getScanRangeLocations(
                    "hdfs://HDFS8000871/usr/hive/warehouse/clickbench.db/hits_orc/part-00128-3e24f7d5-f658-4a80-a168-7b215c5a35bf-c000.snappy.orc",
                    0, 21801531);
            
            tScanRangeLocationsList.add(scanRangeLocations);
        }
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        policy.setScanRangeLocationsList(tScanRangeLocationsList);
        int backendNum = 3;
        Assertions.assertEquals(policy.numBackends(), backendNum);
        for (TScanRangeLocations scanRangeLocations : tScanRangeLocationsList) {
            System.out.println(policy.getNextConsistentBe(scanRangeLocations).getId());
        }
    }

    private TScanRangeLocations getScanRangeLocations(String path, long startOffset, long size) {
        // Generate on file scan range
        TFileScanRange fileScanRange = new TFileScanRange();
        // Scan range
        TExternalScanRange externalScanRange = new TExternalScanRange();
        externalScanRange.setFileScanRange(fileScanRange);
        TScanRange scanRange = new TScanRange();
        scanRange.setExtScanRange(externalScanRange);
        scanRange.getExtScanRange().getFileScanRange().addToRanges(createRangeDesc(path, startOffset, size));
        // Locations
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(scanRange);
        return locations;
    }

    private TFileRangeDesc createRangeDesc(String path, long startOffset, long size) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setPath(path);
        rangeDesc.setStartOffset(startOffset);
        rangeDesc.setSize(size);
        return rangeDesc;
    }
}
