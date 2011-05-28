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
	
	public static final int BLOCKHEADERSIZE = 14;
	
	private BlockType type;
	private int size;
	private long nextBlock;
	
	public BlockHeader() {
		type = BlockType.FREE;
		size = 0;
		nextBlock = 0;
	}
	
	public BlockHeader(BlockType blockType, int blockSize, long nextBlockOffset) {
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
	
	public void readBlock(RandomAccessFile file) throws IOException {
		int ordType = file.readShort();
		type = BlockType.values()[ordType];
		size = file.readInt();
		nextBlock = file.readLong();
	}
	
	public void writeBlock(RandomAccessFile file) throws IOException {
		file.writeShort(type.ordinal());
		file.writeInt(size);
		file.writeLong(nextBlock);
	}
}