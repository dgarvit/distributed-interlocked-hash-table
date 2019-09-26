use AtomicObjects;
use LockFreeStack;
use LockFreeQueue;
use EpochManager;
use Random;
use BlockDist;
use VisualDebug;
use CommDiagnostics;

param BUCKET_UNLOCKED = 0;
param BUCKET_LOCKED = 1;
param BUCKET_DESTROYED = 2;
config param BUCKET_NUM_ELEMS = 8;
config const DEFAULT_NUM_BUCKETS = 1024;
config param MULTIPLIER_NUM_BUCKETS : real = 2;
config param DEPTH = 2;
config param EMAX = 4;

// Note: Once this becomes distributed, we have to make it per-locale
// var seedRNG = new owned RandomStream(uint(64), parSafe=true);

const E_AVAIL = 1;
const E_LOCK = 2;
const P_INNER = 3;
const P_TERM = 4;
const P_LOCK = 5;
const GARBAGE = 6;

class DeferredNode {
	type eltType;
	var val : eltType?;
	var prev : unmanaged DeferredNode(eltType?)?;
	var next : unmanaged DeferredNode(eltType?)?;

	proc init(type eltType) {
		this.eltType = eltType;
	}

	proc init(val : ?eltType) {
		this.eltType = eltType;
		this.val = val;
	}

	proc deinit() {
		var prevNode = prev;
		var nextNode = next;
		if (prevNode == nil) {
			if (nextNode != nil) then nextNode.prev = nil;
		} else {
			if (nextNode == nil) then prevNode.next = nil;
			else {
				prevNode.next = nextNode;
				nextNode.prev = prevNode;
			}
		}
	}
}

class StackNode {
	type eltType;
	var val : eltType?;
	var next : unmanaged StackNode(eltType?)?;

	proc init(type eltType) {
		this.eltType = eltType;
	}

	proc init(val : ?eltType) {
		this.eltType = eltType;
		this.val = val;
	}
}

class Stack {
	type eltType;
	var top : unmanaged StackNode(eltType?)?;
	var count : int;

	proc init(type eltType) {
		this.eltType = eltType;
	}

	proc push(val : eltType?) {
		var node = new unmanaged StackNode(val);
		node.next = top;
		top = node;
		count += 1;
	}

	proc pop() : eltType? {
		if (count > 0) {
			var ret = top.val;
			var next = top.next;
			delete top;
			top = next;
			count -= 1;
			return ret;
		} else {
			var temp : eltType?;
			return temp;
		}
	}

	proc isEmpty() : bool {
		return count == 0;
	}
}

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
	var parent : unmanaged Base(keyType?, valType?)?;

	proc init(type keyType, type valType) {
		this.keyType = keyType;
		this.valType = valType;
	}
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

	proc init(type keyType, type valType) {
		super.init(keyType, valType);
		this.lock.write(E_AVAIL);
	}

	// proc init(parent : unmanaged Buckets(?keyType, ?valType) = nil) {
	// 	super.init(keyType, valType);
	// 	this.lock.write(E_AVAIL);
	// 	this.parent = parent;
	// }

	proc releaseLock() {
		if (lock.read() == E_LOCK) then lock.write(E_AVAIL);
	}
}

class Buckets : Base {
	var seed : uint(64);
	var size : int;
	var bucketsDom = {0..-1};
	var buckets : [bucketsDom] AtomicObject(unmanaged Base(keyType?, valType?)?, hasABASupport=false, hasGlobalSupport=true);
	// var buckets : [0..(size-1)] AtomicObject(unmanaged Base(keyType?, valType?)?, hasABASupport=false, hasGlobalSupport=true);

	proc init(type keyType, type valType, seed : uint(64) = 0) {
		super.init(keyType, valType);
		this.lock.write(P_INNER);
		this.seed = seed;
		this.size = DEFAULT_NUM_BUCKETS;
		this.bucketsDom = {0..#DEFAULT_NUM_BUCKETS};
	}

	// _gen_key will generate the hash on the combined seed and hash of original key
	// which ensures a better distribution of keys from varying seeds.
	proc hash(key : keyType) {
		return _gen_key(chpl__defaultHashCombine(chpl__defaultHash(key), seed, 1));
	}

	proc releaseLock() {
		if (lock.read() == P_LOCK) then lock.write(P_TERM);
	}

	// proc size return buckets.size;
}

// Wrapper for distributed array used for the root; essentially creates a 'lifetime' for the
// array that we will be keeping a reference to. This makes use of the `pragma "no copy"`
// compiler directive that disables the implicit deep-copy. This is very 'hacky', but it
// has served me very well so far.
config const ROOT_BUCKETS_SIZE = DEFAULT_NUM_BUCKETS * Locales.size;
class RootBucketsArray {
	type keyType;
	type valType;
	var D = {0..#ROOT_BUCKETS_SIZE} dmapped Block(boundingBox={0..#ROOT_BUCKETS_SIZE}, targetLocales=Locales);
	var A : [D] AtomicObject(unmanaged Base(keyType?, valType?)?, hasABASupport=false, hasGlobalSupport=true);
}

pragma "always RVF"
record DistributedMap {
	type keyType;
	type valType;
	var _pid : int = -1;
	

	proc init(type keyType, type valType) {
		this.keyType = keyType;
		this.valType = valType;
		this.complete();
		this._pid = (new unmanaged DistributedMapImpl(keyType, valType)).pid;
	}

	proc destroy() {
		coforall loc in Locales do on loc {
			delete chpl_getPrivatizedCopy(unmanaged DistributedMapImpl, _pid);
		}
	}

	forwarding chpl_getPrivatizedCopy(unmanaged DistributedMapImpl(keyType, valType), _pid);
}

class DistributedMapImpl {
	type keyType;
	type valType;
	var pid : int;
	var rootSeed : uint(64); // Same across all nodes...
	var rootArray : unmanaged RootBucketsArray(keyType, valType);
	var rootBuckets = _newArray(rootArray.A._value);
	var manager : EpochManager;
	var seedRNG = new owned RandomStream(uint(64), parSafe=true);

	proc init(type keyType, type valType) {
		this.keyType = keyType;
		this.valType = valType;
		this.rootArray = new unmanaged RootBucketsArray(keyType, valType);
		this.manager = new EpochManager(); // This will be shared across all instances...
		// TODO: We need to add a `UninitializedEpochManager` helper function that will not initialize the `record`
		// since records are initialzied by default in Chapel, regardless of what you want, with no way to avoid this.

		this.complete();
		this.rootSeed = seedRNG.getNext();
		this.pid = _newPrivatizedClass(this);
	}

	proc init(other, privatizedData) {
		this.keyType = other.keyType;
		this.valType = other.valType;
		this.pid = privatizedData[1];
		this.rootSeed = privatizedData[4];
		this.rootArray = privatizedData[3];
		this.manager = privatizedData[2];
	}

	proc getToken() : owned DistTokenWrapper {
		return manager.register();
	}

	proc rootHash(key : keyType) {
		return _gen_key(chpl__defaultHashCombine(chpl__defaultHash(key), this.rootSeed, 1));
	}

	proc getEList(key : keyType, isInsertion : bool, tok) {
		var curr : unmanaged Buckets(keyType, valType)? = nil;
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
		var shouldYield = false;
		while (true) {
			var next = rootBuckets[idx].read();
			if (next == nil) {
				// If we're not inserting something, I.E we are removing
				// or retreiving, we are done.
				if !isInsertion then return nil;

				// Otherwise, speculatively create a new bucket to add in.
				var newList = new unmanaged Bucket(this.keyType, this.valType);
				newList.lock.write(E_LOCK);

				// We set our Bucket, we also own it so return it
				if (this.rootBuckets[idx].compareAndSwap(nil, newList)) {
					return newList;
				} else {
					// Someone else set their bucket, reload.
					delete newList;
				}
			}
			else if (next.lock.read() == P_INNER) {
				curr = next : unmanaged Buckets(keyType, valType);
				break;
			}
			else if (next.lock.read() == E_AVAIL) {
				// We now own the bucket...
				if (next.lock.compareAndSwap(E_AVAIL, E_LOCK)) {
					// Non-insertions don't care.
					if !isInsertion then return next : unmanaged Bucket(keyType, valType);
					// Insertions cannot have a full bucket...
					// If it is not full return it
					var bucket = next : unmanaged Bucket(keyType, valType)?;
					if bucket.count < BUCKET_NUM_ELEMS then
						return bucket;

					for k in bucket.keys {
						if k == key {
							return bucket;
						}
					}

					// Rehash into new Buckets
					var newBuckets = new unmanaged Buckets(keyType, valType, seedRNG.getNext());
					for (k,v) in zip(bucket.keys, bucket.values) {
						var idx = (newBuckets.hash(k) % newBuckets.size:uint):int;
						if newBuckets.buckets[idx].read() == nil {
							newBuckets.buckets[idx].write(new unmanaged Bucket(keyType, valType));
						}
						var buck = newBuckets.buckets[idx].read() : unmanaged Bucket(keyType, valType)?;
						buck.count += 1;
						buck.keys[buck.count] = k;
						buck.values[buck.count] = v;
					}

					next.lock.write(GARBAGE);
					tok.deferDelete(next);
					rootBuckets[idx].write(newBuckets: unmanaged Base(keyType, valType));
					curr = newBuckets;
					break;
				}
			}

			// if next != nil then writeln(next.lock.read());

			if shouldYield then chpl_task_yield(); // If lock could not be acquired
			shouldYield = true;
		}
		shouldYield = false;

		while (true) {
			var idx = (curr.hash(key) % (curr.buckets.size):uint):int;
			var next = curr.buckets[idx].read();
			if (next == nil) {
				// If we're not inserting something, I.E we are removing
				// or retreiving, we are done.
				if !isInsertion then return nil;

				// Otherwise, speculatively create a new bucket to add in.
				var newList = new unmanaged Bucket(keyType, valType);
				newList.lock.write(E_LOCK);

				// We set our Bucket, we also own it so return it
				if (curr.buckets[idx].compareAndSwap(nil, newList)) {
					return newList;
				} else {
					// Someone else set their bucket, reload.
					delete newList;
				}
			}
			else if (next.lock.read() == P_INNER) {
				curr = next : unmanaged Buckets(keyType, valType);
			}
			else if (next.lock.read() == E_AVAIL) {
				// We now own the bucket...
				if (next.lock.compareAndSwap(E_AVAIL, E_LOCK)) {
					// Non-insertions don't care.
					if !isInsertion then return next : unmanaged Bucket(keyType, valType);
					// Insertions cannot have a full bucket...
					// If it is not full return it
					var bucket = next : unmanaged Bucket(keyType, valType)?;
					if bucket.count < BUCKET_NUM_ELEMS then
						return bucket;

					for k in bucket.keys {
						if k == key {
							return bucket;
						}
					}

					// Rehash into new Buckets
					var newBuckets = new unmanaged Buckets(keyType, valType, seedRNG.getNext());
					for (k,v) in zip(bucket.keys, bucket.values) {
						var idx = (newBuckets.hash(k) % newBuckets.size:uint):int;
						if newBuckets.buckets[idx].read() == nil {
							newBuckets.buckets[idx].write(new unmanaged Bucket(keyType, valType));
						}
						var buck = newBuckets.buckets[idx].read() : unmanaged Bucket(keyType, valType)?;
						buck.count += 1;
						buck.keys[buck.count] = k;
						buck.values[buck.count] = v;
					}

					next.lock.write(GARBAGE);
					tok.deferDelete(next); // tok could be from another locale... Overhead?
					curr.buckets[idx].write(newBuckets: unmanaged Base(keyType, valType));
					curr = newBuckets;
				}
			}

			if shouldYield then chpl_task_yield(); // If lock could not be acquired
			shouldYield = true;
		}
		return nil;
	}

	proc insert(key : keyType, val : valType, tok : owned DistTokenWrapper = getToken()) {
		tok.pin();
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
        var _pid = pid;
		on rootBuckets[idx].locale {
			const (_key,_val) = (key, val);
			var done = false;
			var _this = chpl_getPrivatizedCopy(this.type, _pid);
			var elist = _this.getEList(key, true, tok);
			for i in 1..elist.count {
				if (elist.keys[i] == _key) {
					elist.lock.write(E_AVAIL);
					done = true;
					break;
				}
			}
			if (!done) {
				// count.add(1);
				elist.count += 1;
				elist.keys[elist.count] = _key;
				elist.values[elist.count] = _val;
				elist.lock.write(E_AVAIL);
			}
		}
		tok.unpin();
	}

	proc find(key : keyType, tok : owned DistTokenWrapper = getToken()) : (bool, valType) {
		tok.pin();
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
		var res = false;
		var resVal : valType?;
		var _pid = pid;
        on rootBuckets[idx].locale {
			const _key = key;
			var _this = chpl_getPrivatizedCopy(this.type, _pid);
			var elist = _this.getEList(key, false, tok);
			if (elist != nil) {
				for i in 1..elist.count {
					if (elist.keys[i] == _key) {
						(res, resVal) = (true, elist.values[i]);
						break;
					}
				}
				elist.lock.write(E_AVAIL);
			}
		}
		tok.unpin();
		return (res, resVal);
	}

	proc erase(key : keyType, tok : owned DistTokenWrapper = getToken()) {
		tok.pin();
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
		var _pid = pid;
        on rootBuckets[idx].locale {
			const _key = key;
			var _this = chpl_getPrivatizedCopy(this.type, _pid);
			// var (elist, pList, idx) = getPEList(key, false, tok);
			var elist = _this.getEList(key, false, tok);
			if (elist != nil) {
				for i in 1..elist.count {
					if (elist.keys[i] == _key) {
						// count.sub(1);
						elist.keys[i] = elist.keys[elist.count];
						elist.values[i] = elist.values[elist.count];
						elist.count -= 1;
						break;
					}
				}
				elist.lock.write(E_AVAIL);
			}

			// if elist.count == 0 {
			// 	pList.buckets[idx].write(nil);
			// 	elist.lock.write(GARBAGE);
			// 	tok.deferDelete(elist);
			// } else elist.lock.write(E_AVAIL);
		}
		tok.unpin();
	}

	proc dsiPrivatize(privatizedData) {
		return new unmanaged DistributedMapImpl(this, privatizedData);
	}

	proc dsiGetPrivatizeData() {
		return (pid, manager, rootArray, rootSeed);
	}

	inline proc getPrivatizedInstance() {
		return chpl_getPrivatizedCopy(this.type, pid);
	}
}

config const N = 1024 * 1024;
use Time;

proc randomOpsBenchmark (maxLimit : uint = max(uint(16))) {
	var timer = new Timer();
	// startVdebug("E1");
	var map = new DistributedMap(int, int);
	// var tok = map.getToken();
	// tok.pin();
	// map.insert(1..(maxLimit:int), 0, tok);
	// tok.unpin();
	timer.start();
	coforall loc in Locales do on loc {
		coforall tid in 1..here.maxTaskPar {
			var tok = map.getToken();
			tok.pin();
			var rng = new RandomStream(real);
			var keyRng = new RandomStream(int);
			for i in 1..N {
				var s = rng.getNext();
				var key = keyRng.getNext(0, maxLimit:int);
				if s < 0.33 {
					map.insert(key,i,tok);
				} else if s < 0.66 {
					map.erase(key, tok);
				} else {
					map.find(key, tok);
				}
			}
			tok.unpin();
		}
	}
	timer.stop();
	// stopVdebug();
	writeln("Time taken: ", timer.elapsed());
	var ops = Locales.size * here.maxTaskPar * N;
	var opspersec = ops/timer.elapsed();
	writeln("Completed ", ops, " operations in ", timer.elapsed(), "s with ", opspersec, " operations/sec");
}

proc randomOpsStrongBenchmark (maxLimit : uint = max(uint(16))) {
	var timer = new Timer();
	var map = new DistributedMap(int, int);
	// var tok = map.getToken();
	// tok.pin();
	// map.insert(1..(maxLimit:int), 0, tok);
	// tok.unpin();
	timer.start();
	coforall loc in Locales do on loc {
		const opsperloc = N / Locales.size;
		var timer1 = new Timer();
		timer1.start();
		coforall tid in 1..here.maxTaskPar {
			var tok = map.getToken();
			tok.pin();
			var rng = new RandomStream(real);
			var keyRng = new RandomStream(int);
			const opspertask = opsperloc / here.maxTaskPar;
			for i in 1..opspertask {
				var s = rng.getNext();
				var key = keyRng.getNext(0, maxLimit:int);
				if s < 0.33 {
					map.insert(key,i,tok);
				} else if s < 0.66 {
					map.erase(key, tok);
				} else {
					map.find(key, tok);
				}
			}
			tok.unpin();
		}
		timer1.stop();
		writeln(opsperloc / timer1.elapsed());
	}
	timer.stop();
	writeln("Time taken: ", timer.elapsed());
	var opspersec = N/timer.elapsed();
	writeln("Completed ", N, " operations in ", timer.elapsed(), "s with ", opspersec, " operations/sec");
}

proc diagnosticstest() {
	var map = new DistributedMap(int, int);
	var tok = map.getToken();
    startVerboseComms();
	map.insert(1, 1, tok);
	writeln();
	map.find(1, tok);
	writeln();
	map.erase(1, tok);
	stopVerboseComm();
}

config const VERBOSE = false;

proc main() {
	if (VERBOSE) then startVerboseComm();
	randomOpsStrongBenchmark(max(uint(16)));
    if (VERBOSE) then stopVerboseComm();
	// var map = new DistributedMap(int, int);
	// var a : [0..#ROOT_BUCKETS_SIZE] int;
	// var b : [0..#ROOT_BUCKETS_SIZE] int;
	// map.insert(1..1024, 0);
	// for i in 0..#map.rootBuckets.size {
	// 	a[i] = map.rootBuckets[i].locale.id;
	// 	b[i] = map.rootBuckets[i].read().locale.id;
	// 	if (a[i] != b[i]) {
	// 		var s = if map.rootBuckets[i].read() == nil then "nil" else "not nill";
	// 		writeln("Index on ", a[i], " read on ", b[i], " while bucket is ", s);
	// 	}
	// }
	// coforall loc in Locales do on loc {
	// 	for i in 0..#map.rootBuckets.size {
	// 		var x = map.rootBuckets[i].locale.id;
	// 		var y = map.rootBuckets[i].read().locale.id;
	// 		assert(a[i] == x);
	// 		assert(b[i] == y);
	// 	}
	// }
	// diagnosticstest();
	// var map = new DistributedMap(int, int);
	// var tok = map.manager.register();
	// writeln(map);
	// // Test that on Locales other than 0, that they see the same distributed array
	// forall bucket in map.rootBuckets {
	// 	bucket.write(new unmanaged Bucket(int, int));
	// }
	// coforall loc in Locales do on loc {
	// 	assert(&& reduce (map.rootBuckets.read() != nil), here, " has a nil bucket!");
	// }
}
