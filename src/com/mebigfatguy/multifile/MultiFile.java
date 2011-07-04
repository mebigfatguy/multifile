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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class MultiFile {
	static final int BLOCKSIZE = 512;
	
	RandomAccessFile raFile;
	List<DirectoryBlock> directoryBlocks = new ArrayList<DirectoryBlock>();
	List<FreeBlock> freeBlocks = new ArrayList<FreeBlock>();
	
	public MultiFile(String path) throws IOException {
		this(new File(path));
	}
	
	public MultiFile(File file) throws IOException {
		raFile = new RandomAccessFile(file, "rw");
		if (raFile.length() > 0) {
			readDirectory();
			readFreeBlocks();
		} else {
			writeEmptyDirectory();
		}
	}
	
	public void close() throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile already closed");
		}
		
		raFile.close();
		raFile = null;
		directoryBlocks = null;
		freeBlocks = null;
	}
	
	public Collection<String> getStreamNames() throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		Set<String> streamNames = new TreeSet<String>();
		for (DirectoryBlock block : directoryBlocks) {
			streamNames.addAll(block.getStreamNames());
		}
		return streamNames;
	}
	
	public InputStream getReadStream(String streamName) throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		for (DirectoryBlock block : directoryBlocks) {
			Long offset = block.getStreamOffset(streamName);
			if (offset != null) {
				return new MFInputStream(raFile, offset.longValue());
			}
		}
		
		throw new FileNotFoundException("Failed to find stream " + streamName);
	}
	
	public OutputStream getWriteStream(String streamName) throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		long offset = createStream(streamName);
		return new MFOutputStream(raFile, offset);
	}
	
	public void deleteStream(String streamName) throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		for (DirectoryBlock block : directoryBlocks) {
			Long offset = block.removeStream(streamName);
			if (offset != null) {
				block.write(raFile);
				
				FileBlock file = new FileBlock(offset.longValue());
				file.read(raFile);
				FreeBlock free = new FreeBlock(offset.longValue());
				free.write(raFile);
				freeBlocks.add(free);
				
				long next = file.getHeader().getNextBlock();
				while (next != 0) {
					file = new FileBlock(next);
					file.read(raFile);
					free = new FreeBlock(next);
					free.write(raFile);
					freeBlocks.add(free);
					next = file.getHeader().getNextBlock();
				}
				break;
			}
		}
	}
	
	private long createStream(String streamName) throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		deleteStream(streamName);
		
		FreeBlock free = (freeBlocks.isEmpty()) ? null : freeBlocks.remove(freeBlocks.size() - 1);
		long offset;
		if (free != null) {
			offset = free.getOffset();
		} else {
			offset = raFile.length();
		}
		
		for (DirectoryBlock block : directoryBlocks) {
			if (block.addStream(streamName, offset)) {
				block.write(raFile);
				return offset;
			}
		}
		
		DirectoryBlock newBlock = new DirectoryBlock(offset);
		newBlock.addStream(streamName, offset);
		newBlock.write(raFile);
		DirectoryBlock lastBlock = directoryBlocks.get(directoryBlocks.size() - 1);
		lastBlock.setNextOffset(offset);
		lastBlock.write(raFile);
		directoryBlocks.add(newBlock);
		
		return offset;
	}
	
	protected void finalize() throws Throwable {
		try {
			close();
		} catch (IOException ioe) {
		}
		
		super.finalize();
	}
	
	final void readDirectory() throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		DirectoryBlock block = new DirectoryBlock(0);
		block.read(raFile);
		directoryBlocks.add(block);
		
		long nextOffset = block.getNextOffset();
		while (nextOffset != 0) {
			block = new DirectoryBlock(nextOffset);
			block.read(raFile);
			directoryBlocks.add(block);
			nextOffset = block.getNextOffset();
		}
	}
	
	final void readFreeBlocks() throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		long length = raFile.length();
		
		long offset = BLOCKSIZE;
		
		BlockHeader header = new BlockHeader();
		while (offset < length) {
			raFile.seek(offset);
			header.read(raFile);
			if (header.getBlockType() == BlockType.FREE) {
				freeBlocks.add(new FreeBlock(offset));
			}
			offset += BLOCKSIZE;
		}
	}
	
	final void writeEmptyDirectory() throws IOException {
		if (raFile == null) {
			throw new IOException("MultiFile closed");
		}
		
		DirectoryBlock block = new DirectoryBlock(0);
		block.write(raFile);
		directoryBlocks.add(block);
	}
}
