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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.rest.internal.web.RestFunctionTemplate;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, RestAPITest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RestAPIsOnMembersFunctionExecutionDUnitTest extends RestAPITestBase {

  @Parameterized.Parameter
  public String urlContext;

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList("/geode", "/gemfire-api");
  }

  private String createCacheAndRegisterFunction(String hostName, String memberName) {
    final int servicePort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties props = new Properties();
    props.setProperty(NAME, memberName);
    props.setProperty(START_DEV_REST_API, "true");
    props.setProperty(HTTP_SERVICE_BIND_ADDRESS, hostName);
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(servicePort));

    CacheFactory.create(new RestAPIsOnMembersFunctionExecutionDUnitTest().getSystem(props));
    FunctionService.registerFunction(new OnMembersFunction());
    FunctionService.registerFunction(new FullyQualifiedFunction());

    return "http://" + hostName + ":" + servicePort + urlContext + "/v1";

  }

  @Test
  public void testFunctionExecutionOnAllMembers() {
    createCacheForVMs();

    for (int i = 0; i < 5; i++) {
      CloseableHttpResponse response =
          executeFunctionThroughRestCall("OnMembersFunction", null, null, null, null, null);
      assertHttpResponse(response, 200, 4);
    }

    assertCorrectInvocationCount("OnMembersFunction", 20, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  private void createCacheForVMs() {
    restURLs.add(vm0.invoke("createCacheAndRegisterFunction",
        () -> createCacheAndRegisterFunction(vm0.getHost().getHostName(), "m1")));
    restURLs.add(vm1.invoke("createCacheAndRegisterFunction",
        () -> createCacheAndRegisterFunction(vm1.getHost().getHostName(), "m2")));
    restURLs.add(vm2.invoke("createCacheAndRegisterFunction",
        () -> createCacheAndRegisterFunction(vm2.getHost().getHostName(), "m3")));
    restURLs.add(vm3.invoke("createCacheAndRegisterFunction",
        () -> createCacheAndRegisterFunction(vm3.getHost().getHostName(), "m4")));
  }

  @Test
  public void testFunctionExecutionEOnSelectedMembers() {
    createCacheForVMs();

    for (int i = 0; i < 5; i++) {
      CloseableHttpResponse response =
          executeFunctionThroughRestCall("OnMembersFunction", null, null, null, null, "m1,m2,m3");
      assertHttpResponse(response, 200, 3);
    }

    assertCorrectInvocationCount("OnMembersFunction", 15, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  @Test
  public void testFunctionExecutionWithFullyQualifiedName() {
    createCacheForVMs();
    // restURLs.add(createCacheAndRegisterFunction(vm0.getHost().getHostName(), "m1"));

    for (int i = 0; i < 5; i++) {
      CloseableHttpResponse response = executeFunctionThroughRestCall(
          "org.apache.geode.rest.internal.web.controllers.FullyQualifiedFunction", null, null, null,
          null, "m1,m2,m3");
      assertHttpResponse(response, 200, 3);
    }

    assertCorrectInvocationCount(
        "org.apache.geode.rest.internal.web.controllers.FullyQualifiedFunction", 15, vm0, vm1, vm2,
        vm3);

    restURLs.clear();
  }

  @Test
  public void testFunctionExecutionOnMembersWithFilter() {
    createCacheForVMs();

    for (int i = 0; i < 5; i++) {
      CloseableHttpResponse response =
          executeFunctionThroughRestCall("OnMembersFunction", null, "key2", null, null, "m1,m2,m3");
      assertHttpResponse(response, 500, 0);
    }

    assertCorrectInvocationCount("OnMembersFunction", 0, vm0, vm1, vm2, vm3);

    restURLs.clear();
  }

  private class OnMembersFunction extends RestFunctionTemplate {

    public static final String Id = "OnMembersFunction";

    @Override
    public void execute(FunctionContext context) {

      invocationCount++;

      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  private class FullyQualifiedFunction extends RestFunctionTemplate {

    public static final String Id =
        "org.apache.geode.rest.internal.web.controllers.FullyQualifiedFunction";

    @Override
    public void execute(FunctionContext context) {

      invocationCount++;

      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public String getId() {
      return Id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

}
