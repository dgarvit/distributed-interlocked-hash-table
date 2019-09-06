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

// Note: Once this becomes distributed, we have to make it per-locale
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
	// If E_AVAIL || E_LOCK, can be cas to `Bucket`
	// if P_INNER, can be cast to `Buckets`
	// if GARBAGE, then reload as its to be destroyed.
	var lock : atomic int;
	// Is always either 'nil' if its the root, or a
	// a 'Buckets', but I cannot make the field of
	// type 'Buckets' as it is not defined yet.
	var parent : unmanaged Base(keyType, valType);
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
  var count : uint;
  var keys : BUCKET_NUM_ELEMS * keyType;
  var values : BUCKET_NUM_ELEMS * valType;

  proc init(parent : unmanaged Buckets(?keyType, ?valType) = nil) {
    super(keyType, valType);
		this.lock.write(E_AVAIL);
    this.seed = seedRNG.getNext();
    this.parent = parent;
  }
}

class Buckets : Base {
  var seed : uint(64);
  var count : uint;
  var bucketsDom = {0..-1};
  var buckets : [bucketsDom] AtomicObject(unmanaged Base(keyType, valType));

  proc init(parent : unmanaged Buckets(?keyType, ?valType) = nil) {
    super(keyType, valType);
		this.lock.write(P_INNER);
    this.parent = parent;
    this.seed = seedRNG.getNext();
    if parent == nil {
      this.bucketsDom = {0..#DEFAULT_NUM_BUCKETS};
    } else {
      this.bucketsDom = {0..#round(parent.buckets.size * MULTIPLIER_NUM_BUCKETS):int};
    }
  }

  proc hash(key : keyType) { // temporary hash function
    var x = chpl__defaultHash(key);
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9:int;
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb:int;
    x = x ^ (x >> 31);
    x = x ^ seed;
    return x;
  }

	proc size return buckets.size;
}

class ConcurrentMap : Base {
  var count : atomic uint;
  var root : unmanaged Buckets(keyType, valType);

  proc init(type keyType, type valType) {
    super(keyType, valType);
    root = new unmanaged Buckets();
    root.lock.write(P_INNER);
  }

  proc getEList(key : keyType, isInsertion : bool) : Bucket? {
    var found : unmanaged Bucket?;
    var curr = root;
    while (true) {
      var idx = curr.hash(key) % curr.buckets.size;
      var next = curr.buckets[idx];
      if (next == nil) {
				// If we're not inserting something, I.E we are removing 
				// or retreiving, we are done.
				if !isInsertion then return nil;

				// Otherwise, speculatively create a new bucket to add in.
        var newList = new unmanaged Bucket(curr);
        newList.lock.write(E_LOCK);

				// We set our Bucket, we also own it so return it
        if (curr.buckets[idx].compareExchange(nil, newList)) {
          return newList;
				} else {
					// Someone else set their bucket, reload.
          delete newList;
				}
      }
      else if (next.lock.read() == P_INNER) {
        curr = next : unmanaged Buckets?;
				assert(curr, "Bad cast!");
      }
      else if (next.lock.read() == E_AVAIL) {
				// We now own the bucket...
        if (next.lock.compareExchange(E_AVAIL, E_LOCK)) {
					// Non-insertions don't care.
          if !isInsertion then return next;
					// Insertions cannot have a full bucket...
					// If it is not full return it
          if next.count < BUCKET_NUM_ELEMS then
            return next;

          for k in next.keys {
            if k == key {
              return next;
						}
          }

					// Rehash into new Buckets
					var newBuckets = new unmanaged Buckets(keyType, valType);
					for (k,v) in zip(next.keys, next.values) {
						var idx = newBuckets.hash(key) % newBuckets.buckets.size;
						ref bucketRef = newBuckets[idx];
						if bucketRef.read() == nil {
							bucketRef.write(new unmanaged Bucket(keyType, valType))
						}
						var bucket = bucketRef.read();
						bucket.count += 1;
						bucket.keys[bucket.count] = k;
						bucket.values[bucket.count] = v;
					}
					
					// TODO: Need to pass this to 'EpochManager.deferDelete'
					next.lock.write(GARBAGE);
					curr.buckets[idx] = newBuckets;	
        }
      }
    }
  }
}
