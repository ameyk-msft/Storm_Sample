/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hdfs.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Reference bolt implementation to use Azure Data Lake Storage backend.
 *
 * ADL bolt relies on ADL client that buffers data (4MB by default) before
 * persisting the writes on the server.
 *
 * Data larger than 4MB is broken into chunks, and sent over write as
 * multiple writes. Given the nature of network calls, failures in writes
 * (even with retries) may result in data loss/corruption.
 *
 * This bolt implementation attempts to mitigate the data loss by
 * 1. batching up tuples (in the ADL client's write buffer)
 * 2. Issuing a sync call any time the buffer size is met (as dictated by the Sync policy)
 * 3. ACK'ing or FAIL'ing the tuples only on successful/failed sync operations.
 *
 * This batched mode operation ensures atomic writes for tuples, by making sure they
 * are not broken into chunks, and ACK'ing only when the sync succeeds.
 */
public class AdlBolt extends AbstractHdfsBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AdlBolt.class);

	private transient FSDataOutputStream out;
	private RecordFormat format;

	private long offset = 0;
	private long filerotationOffset = 0L;

	private List<backtype.storm.tuple.Tuple> tupleBuffer;

	public AdlBolt() {
		super();
		this.tupleBuffer = new ArrayList<Tuple>();
	}

	public AdlBolt withFsUrl(String fsUrl) {
		this.fsUrl = fsUrl;
		return this;
	}

	public AdlBolt withConfigKey(String configKey) {
		this.configKey = configKey;
		return this;
	}

	public AdlBolt withFileNameFormat(FileNameFormat fileNameFormat) {
		this.fileNameFormat = fileNameFormat;
		return this;
	}

	public AdlBolt withRecordFormat(RecordFormat format) {
		this.format = format;
		return this;
	}

	public AdlBolt withSyncPolicy(SyncPolicy syncPolicy) {
		this.syncPolicy = syncPolicy;
		return this;
	}

	public AdlBolt withRotationPolicy(FileRotationPolicy rotationPolicy) {
		this.rotationPolicy = rotationPolicy;
		return this;
	}

	public AdlBolt addRotationAction(RotationAction action) {
		this.rotationActions.add(action);
		return this;
	}

	@Override
	public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
		LOG.info("Preparing ADL Bolt...");
		this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
	}

	public void execute(Tuple tuple) {
		byte[] bytes = null;
		int tlength = 0;

		synchronized (this.writeLock) {
			try {
				bytes = this.format.format(tuple);
				tlength = bytes.length;

				LOG.info("Checking sync policy {}", new Date().toString());
				if (this.syncPolicy.mark(tuple, offset + tlength)) {
					syncAndRotate(tuple);
				}
			} catch (IOException e) {
				LOG.info("Failing tuples in buffer {}", tupleBuffer.size());
				for (Tuple t : tupleBuffer) {
					this.collector.fail(t);
				}

				LOG.info("Resetting buffer");
				this.filerotationOffset -= this.offset;
				this.offset = 0;
				this.tupleBuffer.clear();

				LOG.info("Failing current tuple");
				this.collector.fail(tuple);

				return;
			}

			try {
				out.write(bytes);
				this.offset += tlength;
				this.filerotationOffset += tlength;

				tupleBuffer.add(tuple);

			} catch (IOException e) {
				LOG.info("Failing tuple");
				this.collector.fail(tuple);
			}
		}
	}

	@Override
	protected void closeOutputFile() throws IOException {
		this.out.close();
	}

	@Override
	protected Path createOutputFile() throws IOException {
		Path filePath = new Path(this.fileNameFormat.getPath(),
				this.fileNameFormat.getName(this.rotation, System.currentTimeMillis()));
		LOG.info("Using output file: " + filePath.getName());

		this.out = this.fs.create(filePath);
		return filePath;
	}

	private void syncAndRotate(Tuple tuple) throws IOException {
		LOG.info("Executing Hdfs sync");
		long start = System.currentTimeMillis();

		if (this.out instanceof HdfsDataOutputStream) {
			((HdfsDataOutputStream) this.out).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
		} else {
			this.out.hsync();
		}
		long time = System.currentTimeMillis() - start;
        LOG.info("hsync took {} ms.", time);

		LOG.info("Resetting Syncpolicy");
		this.syncPolicy.reset();

		LOG.info("Acking individual tuples {}", tupleBuffer.size());
		for (backtype.storm.tuple.Tuple t : tupleBuffer) {
			this.collector.ack(t);
		}

		this.offset = 0;
		this.tupleBuffer.clear();

		LOG.info("Checking file rotation policy {}");
		if (this.rotationPolicy.mark(tuple, this.filerotationOffset)) {
			LOG.info("Rotating file");
			start = System.currentTimeMillis();

			rotateOutputFile(); // synchronized

			time = System.currentTimeMillis() - start;
	        LOG.info("File rotation took {} ms.", time);

			this.filerotationOffset = 0;
			this.rotationPolicy.reset();
		}
	}
}
