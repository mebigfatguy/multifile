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

class BlockHeader {
	private RandomAccessFile raFile;
	private BlockType type;
	private int size;
	private long nextBlock;
	
	public BlockHeader(RandomAccessFile file) throws IOException {
		raFile = file;
		int blockType = raFile.read();
		type = BlockType.values()[blockType];
		size = raFile.readInt();
		nextBlock = raFile.readLong();
	}
	
	public BlockHeader(RandomAccessFile file, BlockType blockType, int blockSize, long nextBlockOffset) {
		raFile = file;
		type = blockType;
		size = blockSize;
		nextBlock = nextBlockOffset;
	}

	public BlockType getBlockType() {
		return type;
	}

	public int getSize() {
		return size;
	}

	public long getNextBlock() {
		return nextBlock;
	}
	
	public void writeBlock() throws IOException {
		raFile.writeInt(type.ordinal());
		raFile.writeInt(size);
		raFile.writeLong(nextBlock);
	}
}