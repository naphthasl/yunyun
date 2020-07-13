#!/usr/bin/env python
"""
Yunyun is intended to be a simplified persistent data storage system similar
to Python's built in shelve, but with features such as transparent file locking
to allow for multi-threaded and multi-process access safely.

GitHub & Further Information: https://github.com/naphthasl/yunyun

License: MIT (see LICENSE for details)
"""

__author__ = 'Naphtha Nepanthez'
__version__ = '0.0.3'
__license__ = 'MIT' # SEE LICENSE FILE
__all__ = [
    'Interface',
    'MultiblockHandler',
    'Shelve'
]

import os, struct, xxhash, pickle, hashlib, io, math, threading

from filelock import Timeout, FileLock, SoftFileLock
from collections.abc import MutableMapping

class Exceptions(object):
    class BlockNotFound(Exception):
        pass
        
    class WriteAboveBlockSize(Exception):
        pass
        
    class NodeExists(Exception):
        pass
        
    class NodeDoesNotExist(Exception):
        pass
        
    class TargetExists(Exception):
        pass
        
    class InvalidFormat(Exception):
        pass

class AtomicCachingFileLock(FileLock):
    def reset_cache(self):
        self.cache = {}
        self.cache['cells'] = {}
    
    def __init__(self, *args, **kwargs):
        self._original_path = args[0]
        args = list(args)
        args[0] += '.lock'
        self._super_thread_lock = threading.Lock()
        super().__init__(*args, **kwargs)
        self.reset_cache()
        self.handle = None
        
    def _acquire(self, *args, **kwargs):
        self._super_thread_lock.acquire()
        super()._acquire(*args, **kwargs)
        self.reset_cache()
        self.handle = open(self._original_path, 'rb+')
        
    def _release(self, *args, **kwargs):
        self._super_thread_lock.release()
        super()._release(*args, **kwargs)
        self.reset_cache()
        self.handle.close()

class Interface(object):
    _index_header_pattern = '<?IHH'  # 9  bytes
    _index_cell_pattern   = '<?QIHQ' # 22 bytes
    _identity_header      = b'YUN'   # 3 bytes
    
    def __init__(self, path, index_size = 4096, block_size = 4096):
        if block_size < 16:
            raise Exceptions.InvalidFormat(
                'Block size is far too low.'
            )
        elif index_size < 64:
            raise Exceptions.InvalidFormat(
                'Index size is far too low.'
            )
        
        self._index_size = index_size
        self._block_size = block_size
        self._index_headersize = len(self.constructIndex())
        self._index_cellsize = len(self.constructIndexCell())
        self._indexes = self._index_size // self._index_cellsize
        
        self.path = path
        
        new = False
        if (not os.path.isfile(self.path) or os.path.getsize(self.path) == 0):
            open(self.path, 'ab+').close()
            new = True
        else:
            hdr = open(self.path, 'rb').read(len(self._identity_header))
            if hdr != self._identity_header:
                raise Exceptions.InvalidFormat(
                    'Not a YunYun file!'
                )
        
        self.lock = AtomicCachingFileLock(self.path)
        
        if new:
            self.requestFreeIndexCell()
        else:
            # Update block and index sizes
            self.getIndexes()
            
    def constructIndex(self, continuation = 0) -> bytes:
        return self._identity_header + struct.pack(
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
        size: int = 0,
        data: int = 0
    ) -> bytes:
            
        return struct.pack(
            self._index_cell_pattern,
            occupied,
            xxhash.xxh64(key).intdigest(),
            seek,
            size,
            data
        )
        
    def readIndexHeader(self, index: bytes):
        return struct.unpack(
            self._index_header_pattern,
            index[len(self._identity_header):]
        )
    
    def readIndexCell(self, cell: bytes):
        return struct.unpack(
            self._index_cell_pattern,
            cell
        )
        
    def getIndexes(self):
        with self.lock:
            if 'indexes' in self.lock.cache:
                return self.lock.cache['indexes']
            else:
                indexes = []
                
                f = self.lock.handle
                f.seek(0, 2)
                length = f.tell()
                position = 0
                while position < length:
                    f.seek(position)
                    read = self.readIndexHeader(
                        f.read(self._index_headersize)
                    )
                    
                    # Set these here!
                    self._index_size = read[2]
                    self._block_size = read[3]
                    self._indexes = (
                        self._index_size // self._index_cellsize
                    )
                    
                    indexes.append((position, read))
                    continuation = read[1]
                    if read[0]:
                        position = continuation
                    else:
                        break
                
                self.lock.cache['indexes'] = indexes
                return indexes
    
    def getIndexesCells(self):
        with self.lock:
            indexes = self.getIndexes()
                
            f = self.lock.handle
            for x in indexes:
                f.seek(x[0] + self._index_headersize)
                
                for y in range(self._indexes):
                    pos = f.tell()
                    
                    if pos not in self.lock.cache['cells']:
                        read = f.read(self._index_cellsize)
                        self.lock.cache['cells'][pos] = (
                            self.readIndexCell(read)
                        )
                    else:
                        f.seek(self._index_cellsize, 1)
                    
        return self.lock.cache['cells']
    
    def createIndex(self):
        with self.lock:
            indexes = self.getIndexes()
            
            f = self.lock.handle
            f.seek(0, 2)
            length = f.tell()
            
            if len(indexes) > 0:
                f.seek(indexes[-1][0])
                f.write(self.constructIndex(length))
                
            f.seek(0, 2)
            f.write(self.constructIndex())
            f.write(self.constructIndexCell() * self._indexes)
            del self.lock.cache['indexes']
              
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
            f = self.lock.handle
            key_exists = self.keyExists(key)
            if not key_exists:
                key_exists = self.requestFreeIndexCell()
                
                f.seek(key_exists)
                cell = self.readIndexCell(f.read(self._index_cellsize))
                
                blank = b'\x00' * self._block_size
                if cell[2] == 0:
                    f.seek(0, 2)
                    location = f.tell()
                    if hard:
                        f.write(blank)
                    else:
                        f.truncate(location + self._block_size)
                else:
                    location = cell[2]
                
                f.seek(key_exists)
                f.write(self.constructIndexCell(
                    True,
                    key,
                    location,
                    cell[3],
                    cell[4]
                ))
                
                try:
                    del self.lock.cache['cells'][key_exists]
                except KeyError:
                    pass

            valhash = xxhash.xxh64(value).intdigest()

            f.seek(key_exists)
            cell = self.readIndexCell(f.read(self._index_cellsize))
            
            if cell[4] == valhash:
                return
            
            f.seek(key_exists)
            f.write(self.constructIndexCell(
                cell[0],
                key,
                cell[2],
                len(value),
                valhash
            ))
            
            try:
                del self.lock.cache['cells'][key_exists]
            except KeyError:
                pass
            
            f.seek(cell[2])
            f.write(value)
                
    def discardBlock(self, key: bytes):
        with self.lock:
            f = self.lock.handle
            key_exists = self.keyExists(key)
            if key_exists:
                f.seek(key_exists)
                cell = self.readIndexCell(f.read(self._index_cellsize))
                
                f.seek(key_exists)
                f.write(self.constructIndexCell(
                    False,
                    b'',
                    cell[2],
                    cell[3],
                    cell[4]
                ))
                
                try:
                    del self.lock.cache['cells'][key_exists]
                except KeyError:
                    pass
            else:
                raise Exceptions.BlockNotFound('!DELT Key: {0}'.format(
                    key.hex()
                ))
                    
    def changeBlockKey(self, key: bytes, new_key: bytes):
        with self.lock:
            f = self.lock.handle
            key_exists = self.keyExists(new_key)
            if key_exists:
                raise Exceptions.TargetExists('!RENM Key: {0}'.format(
                    new_key.hex()
                ))
            
            key_exists = self.keyExists(key)
            if key_exists:
                f.seek(key_exists)
                cell = self.readIndexCell(f.read(self._index_cellsize))
                
                f.seek(key_exists)
                f.write(self.constructIndexCell(
                    cell[0],
                    new_key,
                    cell[2],
                    cell[3],
                    cell[4]
                ))
                
                try:
                    del self.lock.cache['cells'][key_exists]
                except KeyError:
                    pass
            else:
                raise Exceptions.BlockNotFound('!RENM Key: {0}'.format(
                    key.hex()
                ))
                    
    def readBlock(self, key: bytes):
        with self.lock:
            f = self.lock.handle
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

class MultiblockHandler(Interface):
    def constructNodeBlockKey(self, key: bytes, block: int):
        return b'INODEBLK' + hashlib.sha256(
            key + struct.pack('<I', block)
        ).digest()
    
    def makeNode(self, key: bytes):
        with self.lock:
            if not self.nodeExists(key):
                self._setNodeProperties(key,
                    {
                        'key': key,
                        'blocks': 0,
                        'size': 0
                    }
                )
            else:
                raise Exceptions.NodeExists('!MKNOD Key: {0}'.format(
                    key.hex()
                ))
        
    def removeNode(self, key: bytes):
        with self.lock:
            if not self.nodeExists(key):
                raise Exceptions.NodeDoesNotExist('!RMNOD Key: {0}'.format(
                    key.hex()
                ))
            
            details = self._getNodeProperties(key)
            self.discardBlock(key)
        
            for block in range(details['blocks']):
                self.discardBlock(self.constructNodeBlockKey(key, block))
                
    def renameNode(self, key: bytes, new_key: bytes):
        with self.lock:
            if not self.nodeExists(key):
                raise Exceptions.NodeDoesNotExist('!MVNOD Key: {0}'.format(
                    key.hex()
                ))
            elif self.nodeExists(new_key):
                raise Exceptions.TargetExists('!MVNOD Key: {0}'.format(
                    new_key.hex()
                ))
                
            details = self._getNodeProperties(key)
            self.changeBlockKey(key, new_key)
            
            for block in range(details['blocks']):
                self.changeBlockKey(
                    self.constructNodeBlockKey(key, block),
                    self.constructNodeBlockKey(new_key, block)
                )
                
    def nodeExists(self, key: bytes) -> bool:
        with self.lock:
            return bool(self.keyExists(key))
        
    def getHandle(self, key: bytes):
        with self.lock:
            if self.nodeExists(key):
                return self.MultiblockFileHandle(self, key)
            else:
                raise Exceptions.NodeDoesNotExist('!GTHDL Key: {0}'.format(
                    key.hex()
                ))
        
    def _getNodeProperties(self, key: bytes) -> dict:
        return pickle.loads(self.readBlock(key))
    
    def _setNodeProperties(self, key: bytes, properties: dict):
        self.writeBlock(key, pickle.dumps(properties))

    class MultiblockFileHandle(object):
        def __init__(self, interface, key: bytes):
            self.interface = interface
            self.key = key
            self.position = 0
            
        def close(self):
            pass
            
        def tell(self) -> int:
            with self.interface.lock:
                return self.position
            
        def seek(self, offset: int, whence: int = 0) -> int:
            with self.interface.lock:
                if whence == 0:
                    self.position = offset
                elif whence == 1:
                    self.position += offset
                elif whence == 2:
                    if offset != 0:
                        raise io.UnsupportedOperation(
                            'can\'t do nonzero end-relative seeks'
                        )
                    
                    self.position = self.length()
                    
                return self.position
                
        def length(self):
            with self.interface.lock:
                return self.interface._getNodeProperties(
                    self.key
                )['size']
            
        def truncate(self, size: int = None) -> int:
            with self.interface.lock:
                current_size = self.length()
                
                if size == None:
                    size = current_size
                    
                final_blocks = math.ceil(
                    size / self.interface._block_size
                )
                
                current_blocks = math.ceil(
                    current_size / self.interface._block_size
                )
                
                if final_blocks > current_blocks:
                    for block in range(final_blocks):
                        key = self.interface.constructNodeBlockKey(
                            self.key, block
                        )
                        
                        if not self.interface.keyExists(key):
                            self.interface.writeBlock(
                                key,
                                b'\x00' * self.interface._block_size
                            )
                elif final_blocks < current_blocks:
                    for block in range(final_blocks, current_blocks):
                        key = self.interface.constructNodeBlockKey(
                            self.key, block
                        )
                        
                        self.interface.discardBlock(key)
                        
                props = self.interface._getNodeProperties(
                    self.key
                )
                
                props['size'] = size
                props['blocks'] = final_blocks
                
                self.interface._setNodeProperties(
                    self.key,
                    props
                )
                
        def read(self, size: int = None) -> bytes:
            with self.interface.lock:
                if size == None:
                    size = self.length() - self.position
                    
                final = self._readrange(self.position, self.position + size)
                self.position += size
                    
                return final
            
        def _readrange(
            self,
            start: int,
            end: int,
            nopad: bool = True,
            edge: bool = False
        ) -> bytes:
                
            with self.interface.lock:
                start_block = math.floor(start / self.interface._block_size)
                
                end_block = math.ceil(end / self.interface._block_size)
                
                if edge:
                    collect_range = [start_block, end_block - 1]
                else:
                    collect_range = range(start_block, end_block)
                
                blocks = []
                for block in collect_range:
                    key = self.interface.constructNodeBlockKey(
                        self.key, block
                    )
                    
                    blocks.append(self.interface.readBlock(key))
                    
                final = b''.join(blocks)
                    
                if nopad:
                    clean_start = start - (
                        start_block * self.interface._block_size
                    )
                    clean_end = clean_start + (end - start)
                    
                    return final[(clean_start):(clean_end)]
                else:
                    return final
                
        def write(self, b: bytes):
            with self.interface.lock:
                if self.length() < self.position + len(b):
                    self.truncate(self.position + len(b))
                    
                start_block = math.floor(
                    self.position / self.interface._block_size
                )
                
                end_block = math.ceil(
                    (self.position + len(b)) / self.interface._block_size
                )
                    
                chunk_buffer = bytearray(self._readrange(
                    self.position,
                    self.position + len(b),
                    nopad = False,
                    edge = True
                ))
                
                clean_start = self.position - (
                    start_block * self.interface._block_size
                )
                
                chunk_buffer[(clean_start):(clean_start + len(b))] = b
                
                ipos = 0
                for block in range(start_block, end_block):
                    key = self.interface.constructNodeBlockKey(
                        self.key, block
                    )
                    
                    self.interface.writeBlock(
                        key,
                        bytes(
                            chunk_buffer[ipos:ipos+self.interface._block_size]
                        )
                    )
                    
                    ipos += self.interface._block_size
                    
                self.position += len(b)
                
                return len(b)

class Shelve(MutableMapping):
    def __init__(self, *args, **kwargs):
        self.mapping = MultiblockHandler(*args, **kwargs)
        
        if self.mapping._block_size < 96:
            raise Exceptions.InvalidFormat(
                'Shelve mapping block size must be at least 96 bytes.'
            )
        
        self._key_node_name = self._hash_key(b'__KEYS__')
        
        with self.mapping.lock:
            first = False
            if not self.mapping.nodeExists(self._key_node_name):
                self.mapping.makeNode(self._key_node_name)
                first = True
            
            self._ikeys = self.mapping.getHandle(self._key_node_name)
            
            if first:
                self._ikeys.write(pickle.dumps([]))
                self._ikeys.seek(0)
        
    def __getitem__(self, key):
        with self.mapping.lock:
            key = self._hash_key(pickle.dumps(key))
            if not self.mapping.nodeExists(key):
                raise KeyError(key)
            
            return pickle.loads(self.mapping.getHandle(key).read())
        
    def __delitem__(self, key):
        with self.mapping.lock:
            self._ikeys.seek(0)
            kr = pickle.loads(self._ikeys.read())
            
            if key not in kr:
                raise Exceptions.NodeDoesNotExist('!RMNOD Key: {0}'.format(
                    key.hex()
                ))
                
            kr.remove(key)
            fin = pickle.dumps(kr)
            
            self._ikeys.seek(0)
            self._ikeys.truncate(len(fin))
            self._ikeys.write(fin)
            
            key = self._hash_key(pickle.dumps(key))
            self.mapping.removeNode(key)
        
    def __setitem__(self, key, value):
        with self.mapping.lock:
            self._ikeys.seek(0)
            kr = pickle.loads(self._ikeys.read())
            
            if key not in kr:
                kr.append(key)
            fin = pickle.dumps(kr)
            
            self._ikeys.seek(0)
            self._ikeys.truncate(len(fin))
            self._ikeys.write(fin)
            
            key = self._hash_key(pickle.dumps(key))
            if not self.mapping.nodeExists(key):
                self.mapping.makeNode(key)
            
            handle = self.mapping.getHandle(key)
            
            pickval = pickle.dumps(value)
            handle.truncate(len(pickval))
            handle.write(pickval)
        
    def __iter__(self):
        with self.mapping.lock:
            self._ikeys.seek(0)
            kr = pickle.loads(self._ikeys.read())
            
            return iter(kr)
            
    def __len__(self):
        with self.mapping.lock:
            self._ikeys.seek(0)
            kr = pickle.loads(self._ikeys.read())
            
            return len(kr)
        
    def _hash_key(self, key):
        return hashlib.sha256(key).digest()

if __name__ == '__main__':
    import code, progressbar
    
    x = Shelve('test.yun')
    
    for _ in progressbar.progressbar(range(1024)):
        x[os.urandom(8)] = os.urandom(8)
    
    # code.interact(local=dict(globals(), **locals()))
