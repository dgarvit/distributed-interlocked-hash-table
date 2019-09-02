use AtomicObjects;
use EpochManager;
use Random;

param BUCKET_UNLOCKED = 0;
param BUCKET_LOCKED = 1;
param BUCKET_DESTROYED = 2;
config param BUCKET_NUM_ELEMS = 8;
config param DEFAULT_NUM_BUCKETS = 1024;
config param MULTIPLIER_NUM_BUCKETS : real = 2;
config param DEPTH = 2;
config param EMAX = 4;

var seedRNG = new owned RandomStream(uint(64), parSafe=true);

const E_AVAIL = 1;
const E_LOCK = 2;
const P_INNER = 3;
const P_TERM = 4;
const P_LOCK = 5;
const GARBAGE = 6;

// Can be either a singular 'Bucket' or a plural 'Buckets'
class Base {
  type keyType;
  type valType;
}

// Stores keys and values in the hash table. The lock is used to
// determine both the 'lock'/'unlock' state of the bucket, and if
// the bucket is going to be destroyed, meaning that the task should 
// back out and try again. The bucket gets destroyed when a task
// attempts to insert an element into an already-full bucket. All
// tasks _must_ be in the current epoch to even get this far, so
// this Bucket, even if the lock value is BUCKET_DESTROYED, should
// not be destroyed until no it is safe to do so.
class Bucket : Base {
  var lock : atomic int;
  var parent : unmanaged Buckets(keyType, valType);
  var count : uint;
  var keys : BUCKET_NUM_ELEMS * keyType;
  var values : BUCKET_NUM_ELEMS * valType;

  proc init(parent : unmanaged Buckets(?keyType, ?valType) = nil) {
    super(keyType, valType);
    this.seed = seedRNG.getNext();
    this.parent = parent;
  }
}

class Buckets : Base {
  var lock : atomic int;
  var parent : unmanaged Buckets(keyType, valType);
  var seed : uint(64);
  var count : uint;
  var bucketsDom = {0..-1};
  var buckets : [bucketsDom] AtomicObject(unmanaged Bucket(keyType, valType));

  proc init(parent : unmanaged Buckets(?keyType, ?valType) = nil) {
    super(keyType, valType);
    this.parent = parent;
    this.seed = seedRNG.getNext();
    if parent == nil {
      this.bucketsDom = {0..#DEFAULT_NUM_BUCKETS};
    } else {
      this.bucketsDom = {0..#round(parent.buckets.size * MULTIPLIER_NUM_BUCKETS):int};
    }
  }

  proc hash(key : keyType) { // temporary hash function; works only for int
    var x = y;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9:int;
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb:int;
    x = x ^ (x >> 31);
    x = x ^ seed;
    return x;
  }
}

class ConcurrentMap : Base {
  var count : atomic uint;
  var root : unmanaged Buckets(keyType, valType);

  proc init(type keyType, type valType) {
    super(keyType, valType);
    root = new unmanaged Buckets();
    root.lock.write(P_INNER);
  }

  proc getEList(key : keyType) : (Buckets, Buckets) {
    var found : unmanaged Buckets = nil;
    var l : unmanaged Buckets = nil;
    var curr = root;
    while (true) {
      var idx = root.hash(key) % BUCKET_NUM_ELEMS;
      var next = curr.buckets[idx];
      if (next == nil) {
        found = new unmanaged Buckets(curr);
        found.lock.write(E_AVAIL);

        if (l == nil) {
          found.lock.write(E_LOCK);
          l = found;
        }

        if (root.buckets[idx].compareExchange(nil, found)) then
          return (found, l);
      }

      else if (next.lock.read() == P_TERM) {
        if (next.lock.compareExchange(P_TERM, P_LOCK)) {
          curr = next;
          l = curr;
        }
      }

      else if (next.lock.read() == P_INNER) {
        curr = next;
      }

      else if (next.lock.read() == E_AVAIL) {
        if ((l != nil) && next.lock.compareExchange(E_AVAIL, E_LOCK)) {
          if (l == nil) then
            l = next;

          if next.count < EMAX then
            return (next, l);

          for i in 1..next.count {
            if (next.keys[i] == key) then
              return (next, l);
          }
        }
      }

      next.lock.write(GARBAGE);

      if (curr.lock.read() == P_INNER) {
        var p = EToP(next);
      }
    }
  }

  proc EToP(elist : unmanaged Buckets) : unmanaged Buckets {
    var p = new unmanaged Buckets(elist.parent);
    p.lock.write(P_INNER);

    for i in 1..elist.count {

    }
  }
}
