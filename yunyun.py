#!/usr/bin/env python
"""
Yunyun is intended to be a simplified persistent data storage system similar
to Python's built in shelve, but with features such as transparent file locking
to allow for multi-threaded and multi-process access safely.

GitHub & Further Information: https://github.com/naphthasl/yunyun

License: MIT (see LICENSE for details)
"""

__author__ = 'Naphtha Nepanthez'
__version__ = '0.2.3'
__license__ = 'MIT' # SEE LICENSE FILE
__all__ = [
    'Interface',
    'MultiblockHandler',
    'Shelve',
    'InstanceLockedShelve'
]

import os, struct, xxhash, pickle, hashlib, io, math, threading, functools
import zlib, time, random, tempfile

from filelock import FileLock
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

class RRCCache(object):
    def __init__(self, size = 8192):
        self.size = size
        self.foward_mapping = {}
        self.backward_mapping = {}
        
    def set(self, key, value):
        self.foward_mapping[key] = value
        self.backward_mapping[value] = key
        
    def has(self, key):
        return (key in self.foward_mapping)
        
    def keys(self):
        return self.foward_mapping.keys()
        
    def get(self, key):
        return self.foward_mapping[key]
        
    def get_key(self, value):
        return self.backward_mapping[value]
        
    def remove(self, key):
        del self.backward_mapping[self.foward_mapping[key]]
        del self.foward_mapping[key]
        
    def random_key(self):
        return random.choice(self.backward_mapping)
        
    def check_full(self):
        if len(self.foward_mapping) > self.size:
            self.push_out()
            
    def push_out(self):
        self.remove(self.random_key())
        
    def length(self):
        return len(self.foward_mapping)

class ArbRemovalCache(RRCCache):
    def push_out(self):
        # Only works on very latest versions of Python
        self.foward_mapping.popitem()
        self.backward_mapping.popitem()

class OrderedRemovalCache(RRCCache):
    def push_out(self):
        self.remove(list(self.keys())[0])

class CacheDict(MutableMapping):
    def __init__(self, cache, *args, **kwargs):
        self.mapping = cache(*args, **kwargs)
        
    def __getitem__(self, key):
        return self.mapping.get(key)
        
    def __delitem__(self, key):
        self.mapping.remove(key)
        
    def __setitem__(self, key, value):
        self.mapping.set(key, value)
        
    def __iter__(self):
        return iter(self.mapping.keys())
            
    def __len__(self):
        return self.mapping.length()

class AtomicCachingFileLock(FileLock):
    def reset_cache(self):
        self.cache = {}
        self.cache['cells'] = {}
        self.cache['keypos'] = RRCCache()
        self.cache['blocks'] = CacheDict(RRCCache)
        self.cache['index_cell_translation'] = {}
        self.cache['safe_indexes'] = set()
    
    def _cap32(self, i):
        return i & ((2 ** 32) - 1)
    
    def _session(self):
        return struct.pack('<III',
            self._cap32(threading.get_ident()),
            self._cap32(os.getpid()),
            self._cap32(round(time.time() / 3600))
        ) + self.id
    
    def _write_session(self):
        if os.path.getsize(self._original_path) != 0:
            self.handle.seek(0)
            self.handle.write(self._session())
            
    def _same_session(self):
        if os.path.getsize(self._original_path) != 0:
            self.handle.seek(0)
            cses = self._session()
            if self.handle.read(len(cses)) == cses:
                return True
            else:
                return False
        else:
            return False
    
    def _templk(self, i):
        return os.path.join(
            tempfile.gettempdir(), 
            xxhash.xxh64(i).hexdigest() + '.ylf'
        )
    
    def __init__(self, *args, **kwargs):
        args = list(args)
        
        self.id = os.urandom(4)
        
        self._original_path = args[0]
        
        self.lockfile = self._templk(self._original_path + '.lock')
        
        args[0] = self.lockfile
        super().__init__(*args, **kwargs)
        
        self.reset_cache()
        
        self.handle = None
        
    def acquire(self, *args, **kwargs):
        super().acquire(*args, **kwargs)
        
    def _acquire(self, *args, **kwargs):
        super()._acquire(*args, **kwargs)
        
        self.handle = open(self._original_path, 'rb+')
        
        if not self._same_session():
            self.reset_cache()
            self._write_session()
        
    def _release(self, *args, **kwargs):
        super()._release(*args, **kwargs)
        
        self.handle.close()

class Interface(object):
    _index_header_pattern = '<?xxxIHH'  # 12 bytes
    _index_cell_pattern   = '<?xQIHQ'   # 24 bytes
    _yunyun_header        = b'YUNYUN01' # 8  bytes
    _identity_header      = _yunyun_header.rjust(len(_yunyun_header)+16, b'#')
    _cache_size           = 4096
    
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
        self.recalculateIndexes()
        self.reconfigured = False
        
        self.path = path
        
        new = False
        if (not os.path.isfile(self.path) or os.path.getsize(self.path) == 0):
            open(self.path, 'ab+').close()
            new = True
        else:
            hdr = open(self.path, 'rb').read(len(self._identity_header))
            cutlen = len(_yunyun_header)
            if hdr[:-cutlen] != self._identity_header[:-cutlen]:
                raise Exceptions.InvalidFormat(
                    'Not a YunYun file!'
                )
        
        self.lock = AtomicCachingFileLock(self.path)
        
        if new:
            self.requestFreeIndexCell()
        else:
            # Update block and index sizes
            self.getIndexes()
            
    def recalculateIndexes(self):
        self._indexes = math.floor(
            (
                self._index_size - self._index_headersize
            ) / self._index_cellsize
        )
            
    # No caching here in case block/index size changes
    def constructIndex(self, continuation = 0) -> bytes:
        return self._identity_header + struct.pack(
            self._index_header_pattern,
            bool(continuation),
            continuation,
            self._index_size,
            self._block_size
        )
        
    @functools.lru_cache(maxsize=_cache_size)
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
            if 'indexes' not in self.lock.cache:
                self.lock.cache['indexes'] = []
                
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
                    if not self.reconfigured:
                        self._index_size = read[2]
                        self._block_size = read[3]
                        self.recalculateIndexes()
                        self.reconfigured = True
                    
                    self.lock.cache['indexes'].append((position, read))
                    continuation = read[1]
                    if read[0]:
                        position = continuation
                    else:
                        break
                        
            return self.lock.cache['indexes']
    
    @functools.lru_cache(maxsize=_cache_size)
    def generateIndexPositions(self, pos):
        return [
            pos + (self._index_cellsize * y) for y in range(
                self._indexes
            )
        ]
    
    def getIndexesCells(self):
        # TODO: This function is absolutely awful. I did my best to optimize
        # it, but it's still an extremely intensive function to run. It's just
        # super difficult.
        
        with self.lock:
            indexes = self.getIndexes()
                
            f = self.lock.handle
            for x in indexes:
                if x[0] in self.lock.cache['safe_indexes']:
                    continue
                
                index_pointer = x[0] + self._index_headersize
                f.seek(index_pointer)
                
                positions = self.generateIndexPositions(index_pointer)
                
                cells = map(lambda z: (
                    z,
                    self.readIndexCell(f.read(self._index_cellsize))
                ), positions)
                self.lock.cache['cells'].update(cells)
                
                translations = map(lambda z: (z, x[0]), positions)
                self.lock.cache['index_cell_translation'].update(translations)   
                
                self.lock.cache['safe_indexes'].add(x[0])
                    
        return self.lock.cache['cells']
    
    def markCellModified(self, pos):
        with self.lock:
            try:
                self.lock.cache['keypos'].remove(
                    self.lock.cache['keypos'].get_key(pos)
                )
            except KeyError:
                pass
            
            try:
                del self.lock.cache['cells'][pos]
            except KeyError:
                pass
                
            try:
                self.lock.cache['safe_indexes'].remove(
                    self.lock.cache['index_cell_translation'][pos]
                )
            except KeyError:
                pass
    
    def createIndex(self):
        with self.lock:
            indexes = self.getIndexes()
            
            f = self.lock.handle
            f.seek(0, 2)
            length = f.tell()
            
            if len(indexes) > 0:
                f.seek(indexes[-1][0])
                f.write(self.constructIndex(length))
                
            actual_header = self.constructIndex()
            temp = io.BytesIO(bytes(self._index_size))
            temp.write(actual_header)
            temp.write(self.constructIndexCell() * self._indexes)
            temp.seek(0)
            
            f.seek(0, 2)
            f.write(temp.read())
            
            # Originally deleted the whole index cache, but there is no
            # point for that at all - we can just add another blank header
            self.lock.cache['indexes'].append((length, self.readIndexHeader(
                actual_header
            )))
              
    def keyExists(self, key: bytes):
        with self.lock:
            if self.lock.cache['keypos'].has(key):
                return self.lock.cache['keypos'].get(key)
            
            hkey = xxhash.xxh64(key).intdigest()
            
            vlt = 0
            
            # Pypy support
            try:
                cells = reversed(self.getIndexesCells().items())
            except TypeError:
                cells = self.getIndexesCells().items()
            
            for k, v in cells:
                if (v[1] == hkey and v[0] == True):
                    vlt = k
                    
                    break
            
            self.lock.cache['keypos'].set(key, vlt)
            
            return vlt
               
    def requestFreeIndexCell(self):
        with self.lock:
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
                try:
                    self.lock.cache['keypos'].remove(key)
                except KeyError:
                    pass
                
                f.seek(key_exists)
                cell = self.readIndexCell(f.read(self._index_cellsize))
                
                blank = b'\x00' * self._block_size
                if cell[2] == 0:
                    f.seek(0, 2)
                    location = f.tell()
                    if hard:
                        f.write(blank)
                    else:
                        f.truncate(location + len(blank))
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
                
                self.markCellModified(key_exists)
                self.setBlockCacheNewValue(key, blank)
                
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
            
            self.markCellModified(key_exists)
            self.setBlockCacheNewValue(key, value)
            
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
                
                self.markCellModified(key_exists)
                self.invalidateBlockCacheKey(key)
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
                
                self.markCellModified(key_exists)
                self.invalidateBlockCacheKey(key)
            else:
                raise Exceptions.BlockNotFound('!RENM Key: {0}'.format(
                    key.hex()
                ))
              
    def invalidateBlockCacheKey(self, key):
        with self.lock:
            try:
                del self.lock.cache['blocks'][key]
            except KeyError:
                pass
            
    def setBlockCacheNewValue(self, key, value):
        self.lock.cache['blocks'][key] = value
                    
    def readBlock(self, key: bytes):
        with self.lock:
            if key in self.lock.cache['blocks']:
                return self.lock.cache['blocks'][key]
            
            f = self.lock.handle
            key_exists = self.keyExists(key)
            if key_exists:
                f.seek(key_exists)
                cell = self.readIndexCell(f.read(self._index_cellsize))
                
                f.seek(cell[2])
                self.setBlockCacheNewValue(key, f.read(cell[3]))
                return self.lock.cache['blocks'][key]
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
        op = self.readBlock(key)
        blocks, size = struct.unpack('<II', op[:8])
        
        return {
            'key': op[8:],
            'blocks': blocks,
            'size': size
        }
    
    def _setNodeProperties(self, key: bytes, properties: dict):
        self.writeBlock(key, struct.pack('<II',
            properties['blocks'],
            properties['size']
        ) + properties['key'])

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
    _locks = {}
    
    def __init__(self, *args, **kwargs):
        if args[0] not in self._locks:
            self._locks[args[0]] = threading.Lock()
        self._lock = self._locks[args[0]]
            
        self.trackkeys = True
        
        with self._lock:
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
                    self._write_keys(set())
        
    def __getitem__(self, key):
        with self._lock:
            with self.mapping.lock:
                key = self._hash_key(pickle.dumps(key))
                if not self.mapping.nodeExists(key):
                    raise KeyError(key)
                
                return pickle.loads(self.mapping.getHandle(key).read())
        
    def _get_keys(self):
        with self.mapping.lock:
            if 'skeys' not in self.mapping.lock.cache:
                self._ikeys.seek(0)
                self.mapping.lock.cache['skeys'] = pickle.loads(
                    zlib.decompress(self._ikeys.read())
                )
            
            return self.mapping.lock.cache['skeys']
            
    def _write_keys(self, kr):
        with self.mapping.lock:
            try:
                kro = self.mapping.lock.cache['skeys']
            except KeyError:
                kro = None
                
            if kr != kro:
                fin = zlib.compress(pickle.dumps(kr))
                
                self._ikeys.seek(0)
                self._ikeys.truncate(len(fin))
                self._ikeys.write(fin)
                
                try:
                    del self.mapping.lock.cache['skeys']
                except KeyError:
                    pass
        
    def __delitem__(self, key):
        with self._lock:
            with self.mapping.lock:
                if self.trackkeys:
                    kr = self._get_keys()
                    
                    if key not in kr:
                        raise Exceptions.NodeDoesNotExist(
                            '!RMNOD Key: {0}'.format(
                                key.hex()
                            )
                        )
                        
                    kr.remove(key)
                    self._write_keys(kr)
                
                key = self._hash_key(pickle.dumps(key))
                self.mapping.removeNode(key)
        
    def __setitem__(self, key, value):
        with self._lock:
            with self.mapping.lock:
                if self.trackkeys:
                    kr = self._get_keys()
                    
                    kr.add(key)
                    self._write_keys(kr)
                
                key = self._hash_key(pickle.dumps(key))
                if not self.mapping.nodeExists(key):
                    self.mapping.makeNode(key)
                
                handle = self.mapping.getHandle(key)
                
                pickval = pickle.dumps(value)
                handle.truncate(len(pickval))
                handle.write(pickval)
        
    def __iter__(self):
        if self.trackkeys:
            with self._lock:
                with self.mapping.lock:
                    return iter(self._get_keys())
        else:
            return None
            
    def __len__(self):
        if self.trackkeys:
            with self._lock:
                with self.mapping.lock:
                    return len(self._get_keys())
        else:
            return None
        
    def _hash_key(self, key):
        return hashlib.sha256(key).digest()

class InstanceLockedShelve(Shelve):
    """
    This will share the same file lock and cache for all operations, but for as
    long as this object is instantiated, no other thread or process will be
    able to access the shelve.
    
    So, in simple terms, it's faster, but only if you're working with a single
    thread or process.
    
    Can be used with `with`.
    """
    _locks_i = {}
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.mapping.lock.acquire()
        
        if args[0] not in self._locks_i:
            self._locks_i[args[0]] = threading.Lock()
        self._lock_i = self._locks_i[args[0]]
        
        self._lock_i.acquire()
        
    def __enter__(self, *args, **kwargs):
        self.mapping.lock.acquire()
        
        return self
        
    def __exit__(self, *args, **kwargs):
        self.mapping.lock.release(force = True)
        
        if self._lock_i.locked():
            self._lock_i.release()
        
    def __del__(self, *args, **kwargs):
        try:
            self.mapping.lock.release(force = True)
        except:
            pass
            
        try:
            if self._lock_i.locked():
                self._lock_i.release()
        except:
            pass

if __name__ == '__main__':
    import progressbar
    import matplotlib.pyplot as plt
    
    FILENAME = '/tmp/test.yun'
    TEST_VALUES = [os.urandom(2 ** x) for x in range(16)]
        
    def pulverise_original():
        try:
            os.remove(FILENAME)
        except:
            pass
        
    def test(x, dotimes):
        indexes = list(range(dotimes))
        times = []
        
        for _ in progressbar.progressbar(indexes):
            key = os.urandom(8)
            
            start = time.perf_counter()
            operations = 0
            for value in TEST_VALUES:
                x[key] = value
                assert x[key] == value
                operations += 2
            end = ((time.perf_counter() - start) / operations) * 1000
            times.append(end)
            
        return indexes, times
    
    fig, ax = plt.subplots()
    
    pulverise_original()
    sobj = Shelve(FILENAME)
    sobj.trackkeys = False
    ax.plot(*test(sobj, 1024), label='keyless_cooperative')
    
    pulverise_original()
    sobj = Shelve(FILENAME)
    ax.plot(*test(sobj, 1024), label='cooperative')
    
    pulverise_original()
    with InstanceLockedShelve(FILENAME) as sobj:
        ax.plot(*test(sobj, 1024), label='lockhogger')
        
    pulverise_original()
    with InstanceLockedShelve(FILENAME) as sobj:
        sobj.trackkeys = False
        ax.plot(*test(sobj, 1024), label='keyless_lockhogger')
    
    ax.set_xlabel('Created Indexes in Database')
    ax.set_ylabel('Time to perform a single database operation (milliseconds)')
    ax.legend()
    plt.show()
    
    pulverise_original()
    
    # code.interact(local=dict(globals(), **locals()))
