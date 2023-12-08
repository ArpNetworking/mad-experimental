/**
 * Copyright 2023 InscopeMetrics.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.metrics.mad.experimental.sources;

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.test.UnorderedRecordEquality;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Tests for the {@code MergingSource} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class TagDroppingSourceTest {

    @Before
    public void setUp() {
        _mockObserver = Mockito.mock(Observer.class);
        _mockSource = Mockito.mock(Source.class);
        _dropSetBuilder = new TagDroppingSource.DropSet.Builder()
                .setMetricPattern(Pattern.compile(".*"));

        _tagDroppingSourceBuilder = new TagDroppingSource.Builder()
                .setName("TransformingSourceTest")
                .setDropSets(ImmutableList.of(_dropSetBuilder.build()))
                .setSource(_mockSource);
    }

    @Test
    public void testAttach() {
        _tagDroppingSourceBuilder.build();
        Mockito.verify(_mockSource).attach(Mockito.any(Observer.class));
    }

    @Test
    public void testStart() {
        _tagDroppingSourceBuilder.build().start();
        Mockito.verify(_mockSource).start();
    }

    @Test
    public void testStop() {
        _tagDroppingSourceBuilder.build().stop();
        Mockito.verify(_mockSource).stop();
    }

    @Test
    public void testToString() {
        final String asString = _tagDroppingSourceBuilder.build().toString();
        Assert.assertNotNull(asString);
        Assert.assertFalse(asString.isEmpty());
    }

    @Test
    public void testMergingObserverInvalidEvent() {
        final TagDroppingSource.DropSet dropSet = new TagDroppingSource.DropSet.Builder()
                .setMetricPattern(Pattern.compile(".*"))
                .setRemoveDimensions(ImmutableList.of())
                .build();
        final TagDroppingSource droppingSource = new TagDroppingSource.Builder()
                .setName("testMergingObserverInvalidEventTransformingSource")
                .setSource(_mockSource)
                .setDropSets(ImmutableList.of(
                        dropSet))
                .build();
        Mockito.reset(_mockSource);
        new TagDroppingSource.DroppingObserver(
                droppingSource,
                ImmutableList.of(dropSet))
                .notify(OBSERVABLE, "Not a Record");
        Mockito.verifyNoInteractions(_mockSource);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergingMetricMergeMismatchedTypes() {
        final TransformingSource.MergingMetric mergingMetric = new TransformingSource.MergingMetric(
                TestBeanFactory.createMetricBuilder()
                        .setType(MetricType.COUNTER)
                        .build());
        mergingMetric.merge(TestBeanFactory.createMetricBuilder()
                .setType(MetricType.GAUGE)
                .build());
    }

    @Test
    public void testMergingMetricStringRepresentation() {
        final TransformingSource.MergingMetric mergingMetric = new TransformingSource.MergingMetric(
                TestBeanFactory.createMetricBuilder()
                        .setType(MetricType.COUNTER)
                        .build());
        final String str = mergingMetric.toString();
    }

    @Test
    public void testMergeNotMatch() {
        final Record nonMatchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "does_not_match",
                        TestBeanFactory.createMetric()))
                .build();

        final Record actualRecord = mapRecord(nonMatchingRecord);

        assertRecordsEqual(actualRecord, nonMatchingRecord);
    }

    @Test
    public void testModificationPreservesStatistics() {
        _dropSetBuilder.setRemoveDimensions(ImmutableList.of(
                "remove1",
                "remove2"));

        final Statistic minStat = new StatisticFactory().getStatistic("min");
        final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics =
                ImmutableMap.of(minStat,
                        ImmutableList.of(
                                ThreadLocalBuilder.<CalculatedValue<Object>, CalculatedValue.Builder<Object>>buildGeneric(
                                        CalculatedValue.Builder.class,
                                        b -> b.setValue(
                                                ThreadLocalBuilder.build(DefaultQuantity.Builder.class,
                                                b2 -> b2.setValue(1.23d))))));
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new DefaultQuantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .setStatistics(statistics)
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "remove1", "old_value",
                                "remove2", "old_value"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.remove("remove1");
        expectedDimensions.remove("remove2");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new DefaultQuantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .setStatistics(statistics)
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }
    
    @Test
    public void testRemoveDimension() {
        _dropSetBuilder.setRemoveDimensions(ImmutableList.of("remove"));
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new DefaultQuantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "remove", "_value"))
                .build();

        final Record actualRecord = mapRecord(matchingRecord);

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.remove("remove");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new DefaultQuantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    @Test
    public void testRemoveMultipleMatching() {
        _dropSetBuilder.setRemoveDimensions(ImmutableList.of("remove"));
        final Record matchingRecord = TestBeanFactory.createRecordBuilder()
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new DefaultQuantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .setDimensions(
                        ImmutableMap.of(
                                Key.HOST_DIMENSION_KEY, "MyHost",
                                Key.SERVICE_DIMENSION_KEY, "MyService",
                                Key.CLUSTER_DIMENSION_KEY, "MyCluster",
                                "remove", "_value",
                                "remove2", "value"))
                .build();


        final TagDroppingSource.DropSet set1 = _dropSetBuilder.build();
        _dropSetBuilder.setRemoveDimensions(ImmutableList.of("remove2"));
        final TagDroppingSource.DropSet set2 = _dropSetBuilder.build();
        _tagDroppingSourceBuilder.setDropSets(ImmutableList.of(set1, set2));
        final Source transformingSource = _tagDroppingSourceBuilder.build();
        transformingSource.attach(_mockObserver);
        notify(_mockSource, matchingRecord);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(transformingSource), argument.capture());
        final Record actualRecord = argument.getValue();

        final Map<String, String> expectedDimensions = Maps.newHashMap(matchingRecord.getDimensions());
        expectedDimensions.remove("remove");
        expectedDimensions.remove("remove2");
        final Record expectedRecord = TestBeanFactory.createRecordBuilder()
                .setAnnotations(matchingRecord.getAnnotations())
                .setTime(matchingRecord.getTime())
                .setDimensions(ImmutableMap.copyOf(expectedDimensions))
                .setMetrics(ImmutableMap.of(
                        "doesnt_match",
                        TestBeanFactory.createMetricBuilder()
                                .setType(MetricType.GAUGE)
                                .setValues(ImmutableList.of(
                                        new DefaultQuantity.Builder()
                                                .setValue(1.23d)
                                                .setUnit(Unit.BYTE)
                                                .build()))
                                .build()))
                .build();
        assertRecordsEqual(actualRecord, expectedRecord);
    }

    private void assertRecordsEqual(final Record actualRecord, final Record expectedRecord) {
        Assert.assertTrue(
                String.format("expected=%s, actual=%s", expectedRecord, actualRecord),
                UnorderedRecordEquality.equals(expectedRecord, actualRecord));
    }

    private Record mapRecord(final Record record) {
        _tagDroppingSourceBuilder.setDropSets(ImmutableList.of(_dropSetBuilder.build()));
        final Source transformingSource = _tagDroppingSourceBuilder.build();
        transformingSource.attach(_mockObserver);
        notify(_mockSource, record);

        final ArgumentCaptor<Record> argument = ArgumentCaptor.forClass(Record.class);
        Mockito.verify(_mockObserver).notify(Mockito.same(transformingSource), argument.capture());
        return argument.getValue();
    }

    private static void notify(final Observable observable, final Object event) {
        final ArgumentCaptor<Observer> argument = ArgumentCaptor.forClass(Observer.class);
        Mockito.verify(observable).attach(argument.capture());
        for (final Observer observer : argument.getAllValues()) {
            observer.notify(observable, event);
        }
    }

    private Observer _mockObserver;
    private Source _mockSource;
    private TagDroppingSource.Builder _tagDroppingSourceBuilder;
    private TagDroppingSource.DropSet.Builder _dropSetBuilder;

    private static final Observable OBSERVABLE = new Observable() {
        @Override
        public void attach(final Observer observer) {
        }

        @Override
        public void detach(final Observer observer) {
        }
    };
}
