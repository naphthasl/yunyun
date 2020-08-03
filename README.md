# yunyun
Persistent thread-safe/process-safe data storage for Python

PyPi: https://pypi.org/project/yunyundb/

## Why should I care?

Yunyun provides a new implementation of the "shelve" module in the Python
standard library (see https://docs.python.org/3.8/library/shelve.html).

This implementation is cross-platform and doesn't depend on gdbm.

It's also designed to be threadsafe and "process-safe." You can access a
Yunyun shelve from multiple processes or threads and have pretty much no issues
(or atleast, that's how it's intended to work. If you do have any issues,
please create a GitHub issue. This feature was literally the whole reason I
created Yunyun, so it would really suck if it didn't work.)

If you try to use the standard library shelve from multiple processes, it will
tell you that the resource is unavailable.

Yunyun still uses Pickle as a backend though, so *be aware that Yunyun is
still held under all of the limitations of Pickle and all of its associated
potential security issues.*

Also, although I did my best to make it as fast as possible, it's probably
nowhere near SQLite or anything. If you have suggestions pertaining to
performance improvements, please do try to implement them and create a pull
request. I'll take anything as long as it doesn't break existing yunyun
database.

## How do I use it?

Pretty much the same way you'd use shelve, if not a bit easier.

```py
import yunyun

# Open the shelve. This will create a new shelve if it doesn't exist, and
# will throw an exception if you try to open a file that exists but doesn't
# conform to the right format.
my_shelve = yunyun.Shelve('mystuff.yun')

# Store data in the key 'hello'
my_shelve['hello'] = 'world'

# Get data stored by key
print(my_shelve['hello'])

# Delete a key
del my_shelve['hello']

# Use an InstanceLockedShelve to perform a large atomic operation
# No other process or thread will be able to access the shelve until this with
# block exits.
with yunyun.InstanceLockedShelve('mystuff.yun') as my_shelve:
    my_shelve['hello'] = 'world'
    del my_shelve['hello']
    
# Alternatively...
my_shelve = yunyun.InstanceLockedShelve('mystuff.yun')
my_shelve['hello'] = 'world'
del my_shelve['hello']

# Just don't forget to delete the object manually, otherwise the database will
# stay locked forever...
del my_shelve

# Generally speaking though, it's better to just use "with" or the normal
# Shelve object.
```

**IMPORTANT WARNING:** Unlike the standard library shelve, there's no
writeback option. This is due to the whole threadsafe/process-safe goal I
specified earlier. For most people this wouldn't be a problem, but if you're
storing nested dictionaries or lists, then mutating them will not reflect their
changes in the Yunyun file because of C pointers and blah blah blah. Actually,
see the warning in the actual standard library shelve for a better explanation
of this problem.

Off the top of my head, a neat solution to this problem would be to use a sort
of "path" for your keys, like this...

```py
import pickle, yunyun

my_shelve = yunyun.Shelve('mystuff.yun')

my_shelve[pickle.dumps(['path', 'to', 'my', 'hello'])] = 'world'
```

Something like that, anyway. If you've got any good ideas about this too, be
sure to let me know. Basically, try not to store types in yunyun databases that
are passed by reference, like dictionaries, lists, sets, that kind of thing.

Nested dictionaries can probably be replaced by my solution above, and to a
certain extent, so can lists. Maybe sets too. You might prefer some kind of
hacky solution to try and trigger `__setitem__` after you mutate a value
instead, it's up to you really.

## So, what is the performance like?

Good. Good enough, anyway. In my testing, with less than 1024 elements it
managed to outperform [dbm](https://docs.python.org/3/library/dbm.html),
[shelve](https://docs.python.org/3/library/shelve.html) (with gdbm),
[sqlitedict](https://github.com/RaRe-Technologies/sqlitedict) and
[pickleDB](https://pythonhosted.org/pickleDB/). Seems to be a tad slower than
[diskcache](https://pypi.org/project/diskcache/) though, even if my solution
seems to be significantly simpler to both understand and implement. Seriously,
it's dead simple. I'd even go as far as to say my library is a little simpler
than **shelve**, and that's really saying something. I've spent ages working on
the performance of this library, and I think it's close to being as good as it
can possibly get without becoming too complicated. I mean, making something
that performs this well in less than 1000 lines in a single file is pretty
impressive, at least in my opinion. I'm proud of it, anyway.

### Average arbitrary database operation latency against element count
![Performance Benchmark 1](https://raw.githubusercontent.com/naphthasl/yunyun/master/benchmark.png)
As you can see, looking pretty fast so far. The key tells you what each
variation of Yunyun's performance is like - most people will probably just
want to use the `cooperative` mode (which is the default), but if you think
you can improve performance by disabling key tracking (essentially turning the
dictionary into something similar to a hashmap), Yunyun shelves have a
`trackkeys` property that you can change at any point. It is not recommended
to enable/disable this several times on the same shelve though, as any elements
created without key tracking will not be tracked even when you re-enable it.

So, in short, even wih 1024 keys/elements in the database, the highest amount
of time it took to perform a single database operation was 800 microseconds.
That's pretty good! Really good, in fact!

### Latency for specific operation types against element count
![Performance Benchmark 2](https://raw.githubusercontent.com/naphthasl/yunyun/master/benchmark2.png)
With this graph, you can see how scalable each operation is. Deleting elements,
getting them and checking if a key is in the shelve are all extremely fast and
scalable operations. The only one that needs work is setting/creating values,
as it's slower than the rest, even if it's still extremely quick. It also slows
down depending on the amount of elements, which is kind of disappointing, even
if the slowdown would take an extremely large amount of elements to become
noticeable. That said, it needs to be worked on, and I am working on it.

Here's the same graph, tested with 16384 elements instead of 1024 elements and
with smoothing to make the results easier to understand.
![Performance Benchmark 3](https://raw.githubusercontent.com/naphthasl/yunyun/master/benchmark3.png)
