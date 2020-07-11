#!/usr/bin/env python
"""
Yunyun is intended to be a simplified persistent data storage system similar
to Python's built in shelve, but with features such as transparent file locking
to allow for multi-threaded and multi-process access safely.
"""

__author__ = 'Naphtha Nepanthez'
__version__ = '0.0.1'
__license__ = 'MIT' # SEE LICENSE FILE

import os, struct, xxhash

from filelock import Timeout, FileLock, SoftFileLock

class Exceptions(object):
    class BlockNotFound(Exception):
        pass
        
    class WriteAboveBlockSize(Exception):
        pass

class Interface(object):
    _index_header_pattern = '<?IHH' # 9 bytes
    _index_cell_pattern   = '<?QIH' # 14 bytes
    
    def __init__(self, path, index_size = 4096, block_size = 4096):
        self._index_size = index_size
        self._block_size = block_size
        self._index_headersize = len(self.constructIndex())
        self._index_cellsize = len(self.constructIndexCell())
        self._indexes = self._index_size // self._index_cellsize
        
        self.path = path
        
        new = False
        if (not os.path.isfile(self.path) or os.path.getsize(self.path) == 0):
            open(self.path, 'wb+').close()
            new = True
        
        self.lock = FileLock(self.path + '.lock')
        
        if new:
            self.createIndex()
        else:
            # Update block and index sizes
            self.getIndexes()
            
    def constructIndex(self, continuation = 0) -> bytes:
        return struct.pack(
            self._index_header_pattern,
            bool(continuation),
            continuation,
            self._index_size,
            self._block_size
        )
        
    def constructIndexCell(
        self,
        occupied: bool = False,
        key: bytes = b'',
        seek: int = 0,
        size: int = 0
    ) -> bytes:
            
        return struct.pack(
            self._index_cell_pattern,
            occupied,
            xxhash.xxh64(key).intdigest(),
            seek,
            size
        )
        
    def readIndexHeader(self, index: bytes):
        return struct.unpack(
            self._index_header_pattern,
            index
        )
    
    def readIndexCell(self, cell: bytes):
        return struct.unpack(
            self._index_cell_pattern,
            cell
        )
        
    def getIndexes(self):
        indexes = []
        with self.lock:
            with open(self.path, 'rb') as f:
                f.seek(0, 2)
                length = f.tell()
                position = 0
                while position < length:
                    f.seek(position)
                    read = self.readIndexHeader(f.read(self._index_headersize))
                    
                    # Set these here!
                    self._index_size = read[2]
                    self._block_size = read[3]
                    self._indexes = self._index_size // self._index_cellsize
                    
                    indexes.append((position, read))
                    continuation = read[1]
                    if read[0]:
                        position = continuation
                    else:
                        break
        
        return indexes
    
    def getIndexesCells(self):
        cells = {}
        with self.lock:
            indexes = self.getIndexes()
                
            with open(self.path, 'rb+') as f:
                for x in indexes:
                    f.seek(x[0] + self._index_headersize)
                    
                    for y in range(self._indexes):
                        pos = f.tell()
                        read = f.read(self._index_cellsize)
                        cells[pos] = self.readIndexCell(read)
                        
        return cells
    
    def createIndex(self):
        with self.lock:
            indexes = self.getIndexes()
            
            with open(self.path, 'rb+') as f:
                f.seek(0, 2)
                length = f.tell()
                
                if len(indexes) > 0:
                    f.seek(indexes[-1][0])
                    f.write(self.constructIndex(length))
                    
                f.seek(0, 2)
                f.write(self.constructIndex())
                f.write(self.constructIndexCell() * self._indexes)
              
    def keyExists(self, key: bytes):
        with self.lock:
            hkey = xxhash.xxh64(key).intdigest()
            for k, v in self.getIndexesCells().items():
                if (v[1] == hkey
                    and v[0] == True):
                        
                    return k
                    
            return 0
               
    def requestFreeIndexCell(self):
        with self.lock:
            ret = None
            while True:
                for k, v in self.getIndexesCells().items():
                    if (v[0] == False):
                            
                        return k
                        
                self.createIndex()
                
    def writeBlock(self, key: bytes, value: bytes, hard: bool = False):
        if len(value) > self._block_size:
            raise Exceptions.WriteAboveBlockSize(
                'Write length was {0}, block size is {1}'.format(
                    len(value),
                    self._block_size
                )
            )
        
        with self.lock:
            with open(self.path, 'rb+') as f:
                key_exists = self.keyExists(key)
                if not key_exists:
                    key_exists = self.requestFreeIndexCell()
                    
                    f.seek(key_exists)
                    cell = self.readIndexCell(f.read(self._index_cellsize))
                    
                    if cell[2] == 0:
                        f.seek(0, 2)
                        location = f.tell()
                        if hard:
                            f.write(b'\x00' * self._block_size)
                        else:
                            f.truncate(location + self._block_size)
                    else:
                        location = cell[2]
                    
                    f.seek(key_exists)
                    f.write(self.constructIndexCell(
                        True,
                        key,
                        location,
                        cell[3]
                    ))

                f.seek(key_exists)
                cell = self.readIndexCell(f.read(self._index_cellsize))
                
                f.seek(key_exists)
                f.write(self.constructIndexCell(
                    cell[0],
                    key,
                    cell[2],
                    len(value)
                ))
                
                f.seek(cell[2])
                f.write(value)
                
    def discardBlock(self, key: bytes):
        with self.lock:
            with open(self.path, 'rb+') as f:
                key_exists = self.keyExists(key)
                if key_exists:
                    f.seek(key_exists)
                    cell = self.readIndexCell(f.read(self._index_cellsize))
                    
                    f.seek(key_exists)
                    f.write(self.constructIndexCell(
                        False,
                        b'',
                        cell[2],
                        cell[3]
                    ))
                else:
                    raise Exceptions.BlockNotFound('!DELT Key: {0}'.format(
                        key.hex()
                    ))
                    
    def readBlock(self, key: bytes):
        with self.lock:
            with open(self.path, 'rb+') as f:
                key_exists = self.keyExists(key)
                if key_exists:
                    f.seek(key_exists)
                    cell = self.readIndexCell(f.read(self._index_cellsize))
                    
                    f.seek(cell[2])
                    return f.read(cell[3])
                else:
                    raise Exceptions.BlockNotFound('!READ Key: {0}'.format(
                        key.hex()
                    ))

if __name__ == '__main__':
    x = Interface('test.yun')
    
    print(x.getIndexes())
    print(x.keyExists(b''))
    
    key = b'stuff'
    content = b'helloworld'
    
    x.writeBlock(key, content)
    print(x.readBlock(key))
    x.discardBlock(key)
    
    try:
        print(x.readBlock(key))
    except Exception as e:
        print(e)
    
    try:
        x.discardBlock(key)
    except Exception as e:
        print(e)
    
    x.writeBlock(key, content)
