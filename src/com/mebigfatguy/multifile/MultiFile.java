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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MultiFile {
	static final int BLOCKSIZE = 512;
	
	RandomAccessFile raFile;
	Map<String, Long> fileOffsets = new HashMap<String, Long>();
	
	public MultiFile(String path) throws IOException {
		this(new File(path));
	}
	
	public MultiFile(File file) throws IOException {
		raFile = new RandomAccessFile(file, "rw");
		if (raFile.length() > 0) {
			readDirectory();
		} else {
			writeEmptyDirectory();
		}
	}
	
	public void close() throws IOException {
		raFile.close();
	}
	
	public Collection<String> getStreamNames() {
		return Collections.unmodifiableSet(fileOffsets.keySet());
	}
	
	public InputStream getReadStream(String streamName) throws IOException {
		return null;
	}
	
	public OutputStream getWriteStream(String streamName) throws IOException {
		{
			Long offset = fileOffsets.get(streamName);
			if (offset != null) {
				deleteStream(streamName);
			}
		}
		
		long offset = createStream(streamName);
		return new MFOutputStream(raFile, offset);
	}
	
	public void deleteStream(String streamName) throws IOException {
		fileOffsets.remove(streamName);
	}
	
	private long createStream(String streamName) throws IOException {
		long pos = raFile.length();
		fileOffsets.put(streamName, pos);
		return pos;
	}
	
	protected void finalize() throws Throwable {
		try {
			close();
		} catch (IOException ioe) {
		}
		
		super.finalize();
	}
	
	void readDirectory() throws IOException {
		raFile.seek(0);
		BlockHeader header = new BlockHeader();
		header.read(raFile);
		while (header != null) {
			int size = header.getSize();
			while (size > 0) {
				long startFP = raFile.getFilePointer();
				String streamName = raFile.readUTF();
				long offset = raFile.readLong();
				fileOffsets.put(streamName, Long.valueOf(offset));
				size -= raFile.getFilePointer() - startFP;
			}
			
			long next = header.getNextBlock();
			if (next > 0) {
				raFile.seek(next);
				header = new BlockHeader();
				header.read(raFile);
			} else {
				header = null;
			}
		}
	}
	
	void writeEmptyDirectory() throws IOException {
		BlockHeader bt = new BlockHeader(BlockType.DIRECTORY, 0, 0);
		raFile.seek(0);
		bt.write(raFile);
		raFile.setLength(BLOCKSIZE);
	}
}
