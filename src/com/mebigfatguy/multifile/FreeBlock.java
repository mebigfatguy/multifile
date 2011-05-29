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

public class FreeBlock extends AbstractBlock {
	
	long offset;
	
	public FreeBlock(long blockOffset) {
		super(new BlockHeader(BlockType.FREE, 0, 0));
		offset = blockOffset;
	}
	
	@Override
	public void write(RandomAccessFile raFile) throws IOException {
		raFile.seek(offset);
		header.write(raFile);
	}

	@Override
	public void read(RandomAccessFile raFile) throws IOException {
		raFile.seek(offset);
		header = new BlockHeader();
		header.read(raFile);
	}
	
	public long getOffset() {
		return offset;
	}
}
