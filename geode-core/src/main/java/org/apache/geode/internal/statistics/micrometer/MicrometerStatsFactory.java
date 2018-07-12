package org.apache.geode.internal.statistics.micrometer;

import java.io.IOException;
import java.io.Reader;

import org.apache.geode.statistics.StatisticDescriptor;
import org.apache.geode.statistics.Statistics;
import org.apache.geode.statistics.StatisticsFactory;
import org.apache.geode.statistics.StatisticsType;

public class MicrometerStatsFactory implements StatisticsFactory {
  @Override
  public Statistics createStatistics(StatisticsType type) {
    return null;
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId) {
    return null;
  }

  @Override
  public Statistics createStatistics(StatisticsType type, String textId, long numericId) {
    return null;
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type) {
    return null;
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId) {
    return null;
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    return null;
  }

  @Override
  public Statistics[] findStatisticsByType(StatisticsType type) {
    return new Statistics[0];
  }

  @Override
  public Statistics[] findStatisticsByTextId(String textId) {
    return new Statistics[0];
  }

  @Override
  public Statistics[] findStatisticsByNumericId(long numericId) {
    return new Statistics[0];
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units) {
    return null;
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units) {
    return null;
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return null;
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units) {
    return null;
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units) {
    return null;
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return null;
  }

  @Override
  public StatisticDescriptor createIntCounter(String name, String description, String units,
                                              boolean largerBetter) {
    return null;
  }

  @Override
  public StatisticDescriptor createLongCounter(String name, String description, String units,
                                               boolean largerBetter) {
    return null;
  }

  @Override
  public StatisticDescriptor createDoubleCounter(String name, String description, String units,
                                                 boolean largerBetter) {
    return null;
  }

  @Override
  public StatisticDescriptor createIntGauge(String name, String description, String units,
                                            boolean largerBetter) {
    return null;
  }

  @Override
  public StatisticDescriptor createLongGauge(String name, String description, String units,
                                             boolean largerBetter) {
    return null;
  }

  @Override
  public StatisticDescriptor createDoubleGauge(String name, String description, String units,
                                               boolean largerBetter) {
    return null;
  }

  @Override
  public StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return null;
  }

  @Override
  public StatisticsType findType(String name) {
    return null;
  }

  @Override
  public StatisticsType[] createTypesFromXml(Reader reader) throws IOException {
    return new StatisticsType[0];
  }
}
