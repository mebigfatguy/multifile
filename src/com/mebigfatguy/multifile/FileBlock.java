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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class FileBlock implements Block {

	BlockHeader header;
	long offset;
	
	public FileBlock(long blockOffset) {
		header = new BlockHeader(BlockType.FILE, 0, 0);
		offset = blockOffset;
	}
	
	@Override
	public void write(RandomAccessFile raFile) throws IOException {
		raFile.seek(offset);
		header.write(raFile);
		
		if (raFile.getFilePointer() == raFile.length()) {
			raFile.setLength(((offset + MultiFile.BLOCKSIZE) / MultiFile.BLOCKSIZE) * MultiFile.BLOCKSIZE);
		}
	}

	@Override
	public void read(RandomAccessFile raFile) throws IOException {
		raFile.seek(offset);
		header = new BlockHeader();
		header.read(raFile);
	}

	@Override
	public long getNextOffset() {
		return header.getNextBlock();
	}

	@Override
	public void setNextOffset(long offset) {
		header.setNextBlock(offset);
	}
	
	public void readStream(RandomAccessFile raFile, byte[] data) throws IOException {
		raFile.seek(offset + BlockHeader.BLOCKHEADERSIZE);
		int readLen = Math.min(data.length, header.getSize());
		raFile.read(data, 0, readLen);
	}
	
	public void writeStream(RandomAccessFile raFile, byte[] data) throws IOException {
		if (data.length > (MultiFile.BLOCKSIZE - BlockHeader.BLOCKHEADERSIZE)) {
			throw new IllegalArgumentException("Block data to large: " + data.length + " must be less than " + (MultiFile.BLOCKSIZE - BlockHeader.BLOCKHEADERSIZE));	
		}
		
		header.setSize(data.length);
		header.write(raFile);
		raFile.write(data);
	}
}
