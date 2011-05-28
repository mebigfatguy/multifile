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
import java.io.OutputStream;
import java.io.RandomAccessFile;

class MFOutputStream extends OutputStream {

	private RandomAccessFile raFile;
	private long startOffset;
	private long currentPageOffset;
	private long currentOffset;
	
	public MFOutputStream(RandomAccessFile file, long offset) throws IOException {
		raFile = file;
		startOffset = offset;
		currentPageOffset = offset;
		raFile.seek(startOffset);
		currentOffset = startOffset;
		BlockHeader header = new BlockHeader(raFile, BlockType.FILE, 0, 0);
		header.writeBlock();
		currentOffset = raFile.getFilePointer();
		if (currentOffset == raFile.length()) {
			raFile.setLength(startOffset + MultiFile.BLOCKSIZE);
		}
	}
	
	@Override
	public void write(int b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(byte[] b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		long endBlock = ((currentOffset + MultiFile.BLOCKSIZE) / MultiFile.BLOCKSIZE) * MultiFile.BLOCKSIZE;
		int length = (int) (endBlock - currentOffset);
		
		int writeLen = Math.min(length, len);
		raFile.seek(currentOffset);
		raFile.write(b, off, writeLen);
		len -= writeLen;
		currentOffset += writeLen;
	}

	@Override
	public void flush() throws IOException {
	}

	@Override
	public void close() throws IOException {
	}
}