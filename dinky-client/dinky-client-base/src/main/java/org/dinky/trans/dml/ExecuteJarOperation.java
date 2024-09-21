/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.dinky.trans.dml;

import org.dinky.executor.CustomTableEnvironment;
import org.dinky.trans.AbstractOperation;
import org.dinky.trans.ExtendOperation;
import org.dinky.trans.parse.ExecuteJarParseStrategy;
import org.dinky.utils.FlinkStreamEnvironmentUtil;
import org.dinky.utils.URLUtils;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.TableResult;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.lang.Opt;
import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import lombok.Setter;

public class ExecuteJarOperation extends AbstractOperation implements ExtendOperation {

    public ExecuteJarOperation(String statement) {
        super(statement);
    }

    @Override
    public Optional<? extends TableResult> execute(CustomTableEnvironment tEnv) {
        try {
            StreamExecutionEnvironment streamExecutionEnvironment = tEnv.getStreamExecutionEnvironment();
            FlinkStreamEnvironmentUtil.executeAsync(getStreamGraph(tEnv), streamExecutionEnvironment);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return Optional.of(TABLE_RESULT_OK);
    }

    public Pipeline getStreamGraph(CustomTableEnvironment tEnv) {
        // todo
        return getStreamGraph(tEnv, Collections.emptyList());
    }

    public Pipeline getStreamGraph(CustomTableEnvironment tEnv, List<URL> classpaths) {
        // todo 解析执行jar sql的参数
        JarSubmitParam submitParam = JarSubmitParam.build(statement);
        return getStreamGraph(submitParam, tEnv, classpaths);
    }
    // todo 这里要注意per-job和application模式的不同，主要是jar包地址和参数地址
    public static Pipeline getStreamGraph(
            JarSubmitParam submitParam, CustomTableEnvironment tEnv, List<URL> classpaths) {
        SavepointRestoreSettings savepointRestoreSettings = StrUtil.isBlank(submitParam.getSavepointPath())
                ? SavepointRestoreSettings.none()
                : SavepointRestoreSettings.forPath(
                        submitParam.getSavepointPath(), submitParam.getAllowNonRestoredState());
        PackagedProgram program;
        try {
            Configuration configuration = tEnv.getConfig().getConfiguration();
            // todo URLUtils::toFile【用户jar从资源中心下载到本地】
            File file =
                    Opt.ofBlankAble(submitParam.getUri()).map(URLUtils::toFile).orElse(null);
            String submitArgs = Opt.ofBlankAble(submitParam.getArgs()).orElse("");
            if (!PackagedProgramUtils.isPython(submitParam.getMainClass())) {
                // todo 添加用户自定义jar
                tEnv.addJar(file);
            } else {
                // python submit
                submitParam.setArgs("--python " + file.getAbsolutePath() + " " + submitArgs);
                file = null;
            }

            program = PackagedProgram.newBuilder()
                    .setJarFile(file) //todo 设置用户自定义jar包
                    .setEntryPointClassName(submitParam.getMainClass())
                    .setConfiguration(configuration)
                    .setSavepointRestoreSettings(savepointRestoreSettings)
                    // todo 设置用户自定义jar的args
                    .setArguments(extractArgs(submitArgs.trim()).toArray(new String[0]))
                    .setUserClassPaths(classpaths)
                    .build();
            int parallelism = StrUtil.isNumeric(submitParam.getParallelism())
                    ? Convert.toInt(submitParam.getParallelism())
                    : tEnv.getStreamExecutionEnvironment().getParallelism();
            // todo 进入flink client！！！！！！！
            Pipeline pipeline = PackagedProgramUtils.getPipelineFromProgram(program, configuration, parallelism, true);

            // When the UserCodeClassLoader is used to obtain the JobGraph in advance,
            // the code generated by the StreamGraph is compiled.
            // When the JobGraph is obtained again in the future,
            // the already compiled code is used directly to prevent the DinkyClassLoader from failing to compile the
            // code of the user Jar.
            if (pipeline instanceof StreamGraph) {
                ClassLoader dinkyClassLoader = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(program.getUserCodeClassLoader());
                ((StreamGraph) pipeline).getJobGraph();
                Thread.currentThread().setContextClassLoader(dinkyClassLoader);
            }

            program.close();
            return pipeline;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> extractArgs(String args) {
        List<String> programArgs = new ArrayList<>();
        if (StrUtil.isNotEmpty(args)) {
            String[] array = args.split("\\s+");
            Iterator<String> iter = Arrays.asList(array).iterator();
            while (iter.hasNext()) {
                String v = iter.next();
                String p = v.substring(0, 1);
                if (p.equals("'") || p.equals("\"")) {
                    String value = v;
                    if (!v.endsWith(p)) {
                        while (!value.endsWith(p) && iter.hasNext()) {
                            value += " " + iter.next();
                        }
                    }
                    programArgs.add(value.substring(1, value.length() - 1));
                } else {
                    programArgs.add(v);
                }
            }
        }
        return programArgs;
    }

    @Override
    public String asSummaryString() {
        return statement;
    }

    public Pipeline explain(CustomTableEnvironment tEnv) {
        return getStreamGraph(tEnv);
    }

    public Pipeline explain(CustomTableEnvironment tEnv, List<URL> classpaths) {
        return getStreamGraph(tEnv, classpaths);
    }
    // todo 对应flink jar提交sql

    /***
     *
     *EXECUTE JAR WITH (
     * 'uri'='rs:///dw/dw_bondee_log_preprocess-1.0-SNAPSHOT.jar',
     * 'main-class'='com.yidian.data.BondeeLogprocess',
     * 'args'='--conf /opt/dinky/tmp/rs/dw/application.yaml',
     * 'parallelism'='2',
     * 'savepoint-path'=''
     * );
     */
    @Setter
    @Getter
    public static class JarSubmitParam {
        protected JarSubmitParam() {}
        // todo 执行sql jar中jar包的地址
        private String uri;
        private String mainClass;
        // todo 用户自定义jar main参数
        private String args;
        private String parallelism;
        private String savepointPath;
        private Boolean allowNonRestoredState = SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.defaultValue();

        public static JarSubmitParam build(String statement) {
            // todo 通过exec jar sql获取参数信息
            JarSubmitParam submitParam = ExecuteJarParseStrategy.getInfo(statement);
            Assert.notBlank(submitParam.getUri());
            return submitParam;
        }
    }
}
