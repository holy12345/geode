/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TXStateSynchronizationRunnableTest {
  private CancelCriterion cancelCriterion;
  private Runnable beforeCompletion;
  private Runnable afterCompletion;
  private CacheClosedException exception;

  @Before
  public void setUp() {
    exception = new CacheClosedException();

    cancelCriterion = mock(CancelCriterion.class);
    beforeCompletion = mock(Runnable.class);
    afterCompletion = mock(Runnable.class);
  }

  @Test
  public void waitForFirstExecutionThrowsExceptionIfCacheClosed() {
    doThrow(exception).when(cancelCriterion).checkCancelInProgress(any());
    TXStateSynchronizationRunnable runnable =
        new TXStateSynchronizationRunnable(cancelCriterion, beforeCompletion);
    assertThatThrownBy(() -> runnable.waitForFirstExecution()).isSameAs(exception);
  }

  @Test
  public void runSecondRunnableThrowsExceptionIfCacheClosed() {
    doThrow(exception).when(cancelCriterion).checkCancelInProgress(any());
    TXStateSynchronizationRunnable runnable =
        new TXStateSynchronizationRunnable(cancelCriterion, beforeCompletion);
    assertThatThrownBy(() -> runnable.runSecondRunnable(afterCompletion)).isSameAs(exception);
  }

  @Test
  public void doSynchronizationOpsWaitsUntilRunSecondRunnable() {
    TXStateSynchronizationRunnable runnable =
        new TXStateSynchronizationRunnable(cancelCriterion, beforeCompletion);
    new Thread(() -> {
      runnable.runSecondRunnable(afterCompletion);
    }).start();
    runnable.run();
    verify(beforeCompletion, times(1)).run();
    verify(afterCompletion, times(1)).run();
  }

  @Test
  public void doSynchronizationOpsDoesNotRunSecondRunnableIfAborted() {
    TXStateSynchronizationRunnable runnable =
        new TXStateSynchronizationRunnable(cancelCriterion, beforeCompletion);
    runnable.abort();
    new Thread(() -> {
      runnable.runSecondRunnable(afterCompletion);
    }).start();
    runnable.run();
    verify(beforeCompletion, times(1)).run();
    verify(afterCompletion, never()).run();
  }

}
