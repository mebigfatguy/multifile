/*
 * MultiFile - A single file store of multiple streams
 * Copyright 2011 MeBigFatGuy.com
 * Copyright 2011 Dave Brosius
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations
 * under the License.
 */
package com.mebigfatguy.multifile;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DirectoryBlock extends AbstractBlock {

	long offset;
	private Map<String, Long> streamOffsets = new HashMap<String, Long>();
	
	public DirectoryBlock(long blockOffset) {
		super(new BlockHeader(BlockType.DIRECTORY, 0, 0));
		offset = blockOffset;
	}
	
	public void write(RandomAccessFile raFile) throws IOException {
		raFile.seek(offset);
		header.write(raFile);
		for (Map.Entry<String, Long> entry : streamOffsets.entrySet()) {
			raFile.writeUTF(entry.getKey());
			raFile.writeLong(entry.getValue().longValue());
		}
		
		if (raFile.getFilePointer() == raFile.length()) {
			raFile.setLength(((offset + MultiFile.BLOCKSIZE) / MultiFile.BLOCKSIZE) * MultiFile.BLOCKSIZE);
		}
	}
	
	public void read(RandomAccessFile raFile) throws IOException {
		raFile.seek(offset);
		header = new BlockHeader();
		header.read(raFile);
		streamOffsets.clear();
		int length = header.getSize();
		long end = offset + BlockHeader.BLOCKHEADERSIZE + length;
		
		while (raFile.getFilePointer() < end) {
			streamOffsets.put(raFile.readUTF(), Long.valueOf(raFile.readLong()));
		}
	}
	
	public long getNextOffset() {
		return header.getNextBlock();
	}
	public void setNextOffset(long offset) {
		header.setNextBlock(offset);
	}
	public Collection<String> getStreamNames() {
		return Collections.unmodifiableSet(streamOffsets.keySet());
	}
	
	public boolean addStream(String streamName, long streamOffset) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeUTF(streamName);
		dos.writeLong(streamOffset);
		dos.flush();
		int streamEntryLength = baos.toByteArray().length;
		int newBlockLength = header.getSize() + streamEntryLength;
		if (newBlockLength < MultiFile.BLOCKSIZE - BlockHeader.BLOCKHEADERSIZE) {
			header.setSize(newBlockLength);
			streamOffsets.put(streamName, Long.valueOf(streamOffset));
			return true;
		}
		
		return false;
	}
	
	public Long removeStream(String streamName) {
		Long offset = streamOffsets.get(streamName);
		if (offset != null) {
			streamOffsets.remove(streamName);
		}
		
		return offset;
	}
	
	public Long getStreamOffset(String streamName) {
		return streamOffsets.get(streamName);
	}
	
	public String toString() {
		return "DirectoryBlock[OFFSET: " + offset + " SIZE: " + header.getSize() + " NEXT: " + header.getNextBlock() + "]";
	}
}
