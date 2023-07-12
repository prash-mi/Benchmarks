/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery.jdbc.benchmarks;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(value = 1)
//@BenchmarkMode(Mode.SampleTime)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ArrowFacadeVsDeepCopy {
  List<ArrowRecordBatch> recordBatches;
  ArrowSchema arrowSchema;
  BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  // Decoder object will be reused to avoid re-allocation and too much garbage collection.
  private VectorSchemaRoot root;
  private VectorLoader loader;

  public void initVectorSchemaRoot() throws IOException {
    Schema schema =
        MessageSerializer.deserializeSchema(
            new ReadChannel(
                new ByteArrayReadableSeekableByteChannel(
                    arrowSchema.getSerializedSchema().toByteArray())));
    Preconditions.checkNotNull(schema);
    List<FieldVector> vectors = new ArrayList<>();
    for (Field field : schema.getFields()) {
      vectors.add(field.createVector(allocator));
    }
    root = new VectorSchemaRoot(vectors);
    loader = new VectorLoader(root);
  }

  @TearDown
  public void close() {
    root.close();
    allocator.close();
  }

  @Setup
  public void setUp() throws Exception {
    RowReader reader = new RowReader();
    Tuple t = reader.getArrowSchemaAndBatch();
    arrowSchema = (ArrowSchema) t.x();
    recordBatches = (List<ArrowRecordBatch>) t.y();
    initVectorSchemaRoot();
    System.out.println("recordBatches batch cnt " + recordBatches.size());
  }

  @Benchmark
  public void ArrowFacadeIteration(Blackhole blackhole) throws IOException { // processes 130k rows
    long totalRowCnt = 0, hash = 0;
    for (ArrowRecordBatch batch : recordBatches) {
      processBatch(batch);
      for (int rowCnt = 0; rowCnt < root.getRowCount(); rowCnt++) {
        String name = root.getVector("name").getObject(rowCnt).toString();
        String number = root.getVector("number").getObject(rowCnt).toString();
        String state = root.getVector("state").getObject(rowCnt).toString();

        String row = String.format("name: %s, number:%s, state:%s", name, number, state);
        hash += row.hashCode();
        totalRowCnt++;
      }
      root.clear();
    }

    blackhole.consume(hash);
  }

  @Benchmark
  public void ArrowDeepCopyIteration(Blackhole blackhole) throws IOException {
    // Map<String, String> simpleNArrayTree = getNArrayTree(recordBatch);
    long totalRowCnt = 0, hash = 0;
    for (ArrowRecordBatch batch : recordBatches) {

      processBatch(batch);
      List<Map<String, Object>> deepCopiedObj =
          composeDeepCopy(root); // translates current ArrowBatch into a list of objects

      for (Map<String, Object> curRow : deepCopiedObj) {
        String name = curRow.get("name").toString();
        String number = curRow.get("number").toString();
        String state = curRow.get("state").toString();
        String row = String.format("name: %s, number:%s, state:%s", name, number, state);
        hash += row.hashCode();
        totalRowCnt++;
      }
      root.clear();
    }
    blackhole.consume(hash);
  }

  private List<Map<String, Object>> composeDeepCopy(VectorSchemaRoot root) {
    List<Map<String, Object>> deepCopiedObj = new ArrayList<>();
    for (int rowCnt = 0; rowCnt < root.getRowCount(); rowCnt++) {
      Map<String, Object> curRow = new HashMap<>();

      Object name = root.getVector("name").getObject(rowCnt);
      Object number = root.getVector("number").getObject(rowCnt);
      Object state = root.getVector("state").getObject(rowCnt);

      curRow.put("name", name);
      curRow.put("number", number);
      curRow.put("state", state);

      deepCopiedObj.add(curRow);
    }

    return deepCopiedObj;
  }

  public void processBatch(ArrowRecordBatch batch) throws IOException {
    org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch =
        MessageSerializer.deserializeRecordBatch(
            new ReadChannel(
                new ByteArrayReadableSeekableByteChannel(
                    batch.getSerializedRecordBatch().toByteArray())),
            allocator);

    loader.load(deserializedBatch);
    // Release buffers from batch (they are still held in the vectors in root).
    deserializedBatch.close();
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(ArrowFacadeVsDeepCopy.class.getSimpleName()).build();
    new Runner(opt).run();
  }

  class RowReader {

    private class SimpleRowReader implements AutoCloseable {

      @Override
      public void close() { // Benchmarking class will close these
        /*      root.close();
        allocator.close();*/
      }
    }

    public Tuple getArrowSchemaAndBatch() throws Exception {

      String projectId = "java-docs-samples-testing";
      Integer snapshotMillis = null;

      try (BigQueryReadClient client = BigQueryReadClient.create()) {
        String parent = String.format("projects/%s", projectId);

        // This example uses baby name data from the public datasets.
        String srcTable =
            String.format(
                "projects/%s/datasets/%s/tables/%s",
                "bigquery-public-data", "usa_names", "usa_1910_current");

        // We specify the columns to be projected by adding them to the selected fields,
        // and set a simple filter to restrict which rows are transmitted.
        TableReadOptions options =
            TableReadOptions.newBuilder()
                .addSelectedFields("name")
                .addSelectedFields("number")
                .addSelectedFields("state")
                .setRowRestriction("state = \"WA\"")
                .build();

        // Start specifying the read session we want created.
        ReadSession.Builder sessionBuilder =
            ReadSession.newBuilder()
                .setTable(srcTable)
                // This API can also deliver data serialized in Apache Avro format.
                // This example leverages Apache Arrow.
                .setDataFormat(DataFormat.ARROW)
                .setReadOptions(options);

        // Optionally specify the snapshot time.  When unspecified, snapshot time is "now".
        if (snapshotMillis != null) {
          Timestamp t =
              Timestamp.newBuilder()
                  .setSeconds(snapshotMillis / 1000)
                  .setNanos((int) ((snapshotMillis % 1000) * 1000000))
                  .build();
          TableModifiers modifiers = TableModifiers.newBuilder().setSnapshotTime(t).build();
          sessionBuilder.setTableModifiers(modifiers);
        }

        // Begin building the session creation request.
        CreateReadSessionRequest.Builder builder =
            CreateReadSessionRequest.newBuilder()
                .setParent(parent)
                .setReadSession(sessionBuilder)
                .setMaxStreamCount(1);

        ReadSession session = client.createReadSession(builder.build());
        // Setup a simple reader and start a read session.

        // Assert that there are streams available in the session.  An empty table may not have
        // data available.  If no sessions are available for an anonymous (cached) table, consider
        // writing results of a query to a named table rather than consuming cached results
        // directly.
        Preconditions.checkState(session.getStreamsCount() > 0);

        // Use the first stream to perform reading.
        String streamName = session.getStreams(0).getName();

        ReadRowsRequest readRowsRequest =
            ReadRowsRequest.newBuilder().setReadStream(streamName).build();

        // Process each block of rows as they arrive and decode using our simple row reader.
        ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
        List<ArrowRecordBatch> arrowRecordBatches = new ArrayList<>();
        for (ReadRowsResponse response : stream) {
          // System.out.println("ReadRowsResponse response.getRowCount(): "+response.getRowCount());

          Preconditions.checkState(response.hasArrowRecordBatch());
          arrowRecordBatches.add(response.getArrowRecordBatch());
        }
        System.out.println("arrowRecordBatches: " + arrowRecordBatches.size());
        return Tuple.of(session.getArrowSchema(), arrowRecordBatches);
      }
    }
  }
}
