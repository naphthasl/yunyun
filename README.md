# yunyun
Persistent thread-safe/process-safe data storage for Python

PyPi: https://pypi.org/project/yunyundb/0.0.1/

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
sure to let me know.
