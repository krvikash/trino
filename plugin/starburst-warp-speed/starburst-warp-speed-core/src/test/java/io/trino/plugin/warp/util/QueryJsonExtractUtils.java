/*
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
package io.trino.plugin.warp.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import io.airlift.units.Duration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class QueryJsonExtractUtils
{
    private QueryJsonExtractUtils()
    {
    }

    public static void main(String[] args)
            throws IOException
    {
        // TODO: In order to use this util, please update the following args:
        String folderPath = "/tmp/please/set/a/directory/path";
        boolean lastJsonInEachFolder = true;

        File rootDir = new File(folderPath);
        long executionTimeNanos = 0;
        for (File file : getDirFiles(rootDir, lastJsonInEachFolder)) {
            executionTimeNanos += extract(file);
        }
        System.out.println("Total execution time = " + Duration.succinctNanos(executionTimeNanos).toString(TimeUnit.SECONDS));
    }

    private static class JsonFilter
            implements FilenameFilter
    {
        @Override
        public boolean accept(File dir, String name)
        {
            return (name.endsWith(".json") && !name.endsWith("_tree.json") && !name.endsWith("_orig.json") && !name.endsWith("_sort.json")) || new File(dir.getAbsolutePath() + "/" + name).isDirectory();
        }
    }

    private static List<File> getDirFiles(File file, boolean lastJsonInEachFolder)
    {
        List<File> ret = new ArrayList<>();
        File[] files = file.listFiles(new JsonFilter());
        if (files == null) {
            throw new RuntimeException("files is null");
        }
        Stream<File> filesStream = Arrays.stream(files).filter(File::isFile);
        if (lastJsonInEachFolder) {
            filesStream.min((f1, f2) -> -1 * f1.getAbsolutePath().compareTo(f2.getAbsolutePath())).ifPresent(ret::add);
        }
        else {
            filesStream.forEach(ret::add);
        }
        Arrays.stream(files)
                .filter(File::isDirectory)
                .forEach(directory -> ret.addAll(getDirFiles(directory, lastJsonInEachFolder)));
        return ret;
    }

    private static String prepareFileContent(String fileName, boolean shouldTrim)
            throws IOException
    {
        try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
            String content = new String(fileInputStream.readAllBytes(), Charset.defaultCharset());
            if (shouldTrim) {
                content = content.replaceAll("\"\"", "\"");
                if (content.startsWith("\"")) {
                    content = content.substring(1, content.length() - 1);
                }
            }
            return content;
        }
    }

    public static long extract(File f)
            throws IOException
    {
        System.out.println("starting " + f);
        ObjectReader objectReader = new ObjectMapper().readerFor(TreeMap.class);
        String filename = f.getAbsolutePath().substring(0, f.getAbsolutePath().lastIndexOf(".json"));
        ContainerNode jsonNode = (ContainerNode) objectReader.readTree(new StringReader(prepareFileContent(filename + ".json", false)));
        ArrayNode operatorsNode;
        ObjectNode queryStats = (ObjectNode) jsonNode.get("queryStats");
        operatorsNode = (ArrayNode) queryStats.get("operatorSummaries");
        Map<String, ObjectNode> s = new TreeMap<>();
        for (JsonNode node : operatorsNode) {
            ObjectNode convertedNode = convertNode((ObjectNode) node);
            if (convertedNode != null) {
                String key = String.format(Locale.US, "%2d_%2d_%2d_%2d", convertedNode.get("1.stageId").intValue(), convertedNode.get("2.pipelineId").intValue(), convertedNode.get("3.alternativeId").intValue(), convertedNode.get("4.operatorId").intValue());
                s.put(key, convertedNode);
            }
        }
        Map<String, Object> tree = new LinkedHashMap<>();
        addStats(tree, queryStats);
        addOperators(tree, s);
        new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValue(new File(filename + "_tree.json"), tree);
        return getExecutionTimeNanos(queryStats);
    }

    private static long getExecutionTimeNanos(ObjectNode queryStats)
    {
        Duration duration = Duration.valueOf(queryStats.get("executionTime").textValue());
        return duration.roundTo(TimeUnit.NANOSECONDS);
    }

    private static void addStats(Map<String, Object> tree, ObjectNode queryStats)
    {
        List<String> statsToAdd = List.of("executionTime", "planningTime");
        statsToAdd.forEach(stat -> tree.put(stat, queryStats.get(stat)));
    }

    private static void addOperators(Map<String, Object> tree, Map<String, ObjectNode> s)
    {
        s.forEach((key, value) -> {
            List<String> keyTokens = Splitter.on('_').splitToList(key);
            if (keyTokens.size() < 4) {
                throw new RuntimeException("Expected more parts");
            }
            tree.putIfAbsent(getKey(keyTokens.get(0)), new HashMap<>());
            Map<String, Map<String, Map<String, String>>> stageMap = (Map<String, Map<String, Map<String, String>>>) tree.get(getKey(keyTokens.get(0)));
            stageMap.putIfAbsent(getKey(keyTokens.get(1)), new HashMap<>());
            Map<String, Map<String, String>> pipelineMap = stageMap.get(getKey(keyTokens.get(1)));
            pipelineMap.putIfAbsent(getKey(keyTokens.get(2)), new HashMap<>());
            Map<String, String> alternativeMap = pipelineMap.get(getKey(keyTokens.get(2)));
            JsonNode connectorMetrics = value.get("connectorMetrics");
            String moreInfo = "";
            if (connectorMetrics.get("dispatcherPageSource:execution_time") != null) {
                JsonNode executionTimeMetricsNode = connectorMetrics.get("dispatcherPageSource:execution_time");
                moreInfo += ", exec_time=" + executionTimeMetricsNode.get("total").asLong();
                Iterator<Map.Entry<String, JsonNode>> connectorMetricsIter = connectorMetrics.fields();
                while (connectorMetricsIter.hasNext()) {
                    Map.Entry<String, JsonNode> metrics = connectorMetricsIter.next();
                    if (metrics.getKey().contains("TABLE_NAME")) {
                        moreInfo += ", tn=" + metrics.getKey();
                    }
                    else if (metrics.getKey().contains("SCHEMA_NAME")) {
                        moreInfo += ", sn=" + metrics.getKey();
                    }
                    else if (metrics.getKey().equals("dispatcherPageSource:external_collect_columns")) {
                        JsonNode externalCollectMetricsNode = connectorMetrics.get("dispatcherPageSource:external_collect_columns");
                        moreInfo += ", ec=" + externalCollectMetricsNode.get("total").asLong();
                    }
                    else if (metrics.getKey().equals("dispatcherPageSource:external_match_columns")) {
                        JsonNode externalMatchMetricsNode = connectorMetrics.get("dispatcherPageSource:external_match_columns");
                        moreInfo += ", em=" + externalMatchMetricsNode.get("total").asLong();
                    }
                }
            }
            alternativeMap.put(getKey(keyTokens.get(3)), (value.get("5.operatorType").textValue() + ": op=" + value.get("outputPositions") + ": wall=" + value.get("getOutputWall") + ": driver=" + value.get("totalDrivers") + moreInfo));
        });
    }

    private static String getKey(String key)
    {
        return String.format(Locale.US, "%2s", key.trim());
    }

    private static ObjectNode convertNode(ObjectNode node)
    {
        Map<String, JsonNode> nodeValues = new TreeMap<>();
        Iterator<Map.Entry<String, JsonNode>> elements = node.fields();
        while (elements.hasNext()) {
            Map.Entry<String, JsonNode> entry = elements.next();
            String key = entry.getKey();
            switch (key) {
                case "stageId":
                    key = "1.stageId";
                    break;
                case "pipelineId":
                    key = "2.pipelineId";
                    break;
                case "alternativeId":
                    key = "3.alternativeId";
                    break;
                case "operatorId":
                    key = "4.operatorId";
                    break;
                case "operatorType":
                    key = "5.operatorType";
                    break;
                default:
                    break;
            }
            if (key.equals("metrics") || key.equals("info")) {
                continue;
            }
            JsonNode value = entry.getValue();
            if (key.contains("operatorType") && value.asText().contains("Exchange")) {
                return null;
            }
            if (value instanceof ObjectNode objectNode) {
                value = convertNode(objectNode);
            }
            nodeValues.put(key, value);
        }
        ObjectNode ret = new ObjectNode(JsonNodeFactory.instance);
        nodeValues.forEach(ret::replace);
        return ret;
    }
}
