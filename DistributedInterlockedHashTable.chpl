use AtomicObjects;
use LockFreeStack;
use LockFreeQueue;
use EpochManager;
use Random;
use BlockDist;
use VisualDebug;
use CommDiagnostics;
use AggregationBuffer;
use Time;

config param BUCKET_NUM_ELEMS = 8;
config const DEFAULT_NUM_BUCKETS = 1024;
config param MULTIPLIER_NUM_BUCKETS : real = 2;
config param DEPTH = 2;
config param EMAX = 4;
config const FLUSHLOCAL = true;
config const VERBOSE = false;
config const DFS = false;
config const VDEBUG = false;
config const PRINT_TIME = false;
config const GETELIST_COUNT = false;
config const ROOT_BUCKETS_SIZE = DEFAULT_NUM_BUCKETS * Locales.size;
config const BUFFER_SIZE = 8 * 1024;

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

enum MapAction {
	insert,
	find,
	erase
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

    proc readWriteThis(f) {
      f <~> "(ElementList) {\n\tcount=%@xu\n\t".format(count);
      for i in 1..8 {
        f <~> "keys[%i] = %@xu, values[%i] = %@xu\n\t".format(i, keys[i], i, values[i]);
      }
      if parent != nil {
        f <~> "parent = " <~> parent;
      }
      f <~> "\n}";
    }
}

class Buckets : Base {
	var seed : uint(64);
	var bucketsDom = {0..-1};
	var buckets : [bucketsDom] AtomicObject(unmanaged Base(keyType?, valType?)?, hasABASupport=false, hasGlobalSupport=true);
	// var buckets : [0..(size-1)] AtomicObject(unmanaged Base(keyType?, valType?)?, hasABASupport=false, hasGlobalSupport=true);

	proc init(type keyType, type valType, seed : uint(64) = 0, size : int = DEFAULT_NUM_BUCKETS/2) {
		super.init(keyType, valType);
		this.lock.write(P_INNER);
		this.seed = seed;
		this.bucketsDom = {0..#round(size * MULTIPLIER_NUM_BUCKETS):int};
	}

	// _gen_key will generate the hash on the combined seed and hash of original key
	// which ensures a better distribution of keys from varying seeds.
	proc hash(key : keyType) {
		return _gen_key(chpl__defaultHashCombine(chpl__defaultHash(key), seed, 1));
	}

	proc _hash(key) {
		return _gen_key(chpl__defaultHashCombine(key, seed, 1));
	}

	proc releaseLock() {
		if (lock.read() == P_LOCK) then lock.write(P_TERM);
	}

    proc readWriteThis(f) {
      f <~> "(PointerList) {\n\t";
      for (idx, bucket) in zip(bucketsDom, buckets) {
        if bucket.read() != nil {
          f <~> "[%i]: %@xu\n\t".format(idx, bucket.atomicVar.read()); 
        }
      }
      f <~> "\n}";
    }

	// proc size return buckets.size;
}

// Wrapper for distributed array used for the root; essentially creates a 'lifetime' for the
// array that we will be keeping a reference to. This makes use of the `pragma "no copy"`
// compiler directive that disables the implicit deep-copy. This is very 'hacky', but it
// has served me very well so far.
class RootBucketsArray {
	type keyType;
	type valType;
	var D = {0..#ROOT_BUCKETS_SIZE} dmapped Block(boundingBox={0..#ROOT_BUCKETS_SIZE});
	var A : [D] AtomicObject(unmanaged Base(keyType?, valType?)?, hasABASupport=false, hasGlobalSupport=true);
}

class MapFuture {
	type valType;
	var complete = false;
	var found = false;
	var val : valType?;

	proc init (type valType) {
		this.valType = valType;
	}

	proc success (val : valType) {
		found = true;
		this.val = val;
		complete = true;
	}

	proc fail () {
		found = false;
		complete = true;
	}
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
	type msgType = (MapAction, keyType, valType?, unmanaged MapFuture(valType)?);
	var pid : int;
	var rootSeed : uint(64); // Same across all nodes...
	var rootArray : unmanaged RootBucketsArray(keyType, valType);
	var rootBuckets = _newArray(rootArray.A._value);
	var aggregator = UninitializedAggregator(msgType);
	var manager : EpochManager;
	var seedRNG = new owned RandomStream(uint(64), parSafe=true);

	proc init(type keyType, type valType) {
		this.keyType = keyType;
		this.valType = valType;
		this.rootArray = new unmanaged RootBucketsArray(keyType, valType);
		this.aggregator = new Aggregator((msgType), BUFFER_SIZE);
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
		this.aggregator = privatizedData[5];
		this.manager = privatizedData[2];
	}

	proc getToken() : owned DistTokenWrapper {
		return manager.register();
	}

	proc rootHash(key : keyType) {
		return _gen_key(chpl__defaultHashCombine(chpl__defaultHash(key), this.rootSeed, 1));
	}

	proc _rootHash(key) {
		return _gen_key(chpl__defaultHashCombine(key, this.rootSeed, 1));
	}

	proc getEList(key : keyType, isInsertion : bool, tok) {
		var rootCount = 0;
		var curr : unmanaged Buckets(keyType, valType)? = nil;
		const defaultHash = chpl__defaultHash(key);
		var idx = (this._rootHash(defaultHash) % (this.rootBuckets.size):uint):int;
		var shouldYield = false;
		while (true) {
			rootCount += 1;
			var next = rootBuckets[idx].read();
			// if (next != nil) {
			// 	var lock = next.lock.read();
			// 	if (lock == E_AVAIL || lock == E_LOCK) {
			// 		var elist = next : unmanaged Bucket(keyType, valType);
			// 		assert(elist.count <= 8, elist);
			// 	}
			// }
			if (next == nil) {
				// If we're not inserting something, I.E we are removing
				// or retreiving, we are done.
				if !isInsertion {
					if GETELIST_COUNT then writeln(rootCount, " ", rootCount, " ", 0);
					return nil;
				}

				// Otherwise, speculatively create a new bucket to add in.
				var newList = new unmanaged Bucket(this.keyType, this.valType);
				newList.lock.write(E_LOCK);

				// We set our Bucket, we also own it so return it
				if (this.rootBuckets[idx].compareAndSwap(nil, newList)) {
					if GETELIST_COUNT then writeln(rootCount, " ", rootCount, " ", 0);
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
					if !isInsertion {
						if GETELIST_COUNT then writeln(rootCount, " ", rootCount, " ", 0);
						return next : unmanaged Bucket(keyType, valType);
					}
					// Insertions cannot have a full bucket...
					// If it is not full return it
					var bucket = next : unmanaged Bucket(keyType, valType)?;
					if bucket.count < BUCKET_NUM_ELEMS {
						if GETELIST_COUNT then writeln(rootCount, " ", rootCount, " ", 0);
						return bucket;
					}

					for k in bucket.keys {
						if k == key {
							if GETELIST_COUNT then writeln(rootCount, " ", rootCount, " ", 0);
							return bucket;
						}
					}

					// Rehash into new Buckets
					var newBuckets = new unmanaged Buckets(keyType, valType, seedRNG.getNext());
					for (k,v) in zip(bucket.keys, bucket.values) {
						var idx = (newBuckets.hash(k) % newBuckets.buckets.size:uint):int;
						if newBuckets.buckets[idx].read() == nil {
						  var newBucket = new unmanaged Bucket(keyType, valType);
                          newBucket.parent = newBuckets;
                          newBuckets.buckets[idx].write(newBucket);
                        }
						var buck = newBuckets.buckets[idx].read() : unmanaged Bucket(keyType, valType)?;
						buck.count += 1;
						buck.keys[buck.count] = k;
						buck.values[buck.count] = v;
					}

					next.lock.write(GARBAGE);
					// tok.deferDelete(next);
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

		var loopCount = 0;

		idx = (curr._hash(defaultHash) % (curr.buckets.size):uint):int;
		while (true) {
			loopCount += 1;
			assert(curr.buckets.domain.contains(idx), "Bad idx ", idx, " not in domain ", curr.buckets.domain);
      var next = curr.buckets[idx].read();
			// if (next != nil) {
			// 	var lock = next.lock.read();
			// 	if (lock == E_AVAIL || lock == E_LOCK) {
			// 		var elist = next : unmanaged Bucket(keyType, valType);
			// 		assert(elist.count <= 8, elist);
			// 	}
			// }
			if (next == nil) {
				// If we're not inserting something, I.E we are removing
				// or retreiving, we are done.
				if !isInsertion {
					if GETELIST_COUNT then writeln(rootCount + loopCount, " ", rootCount, " ", loopCount);
					return nil;
				}

				// Otherwise, speculatively create a new bucket to add in.
				var newList = new unmanaged Bucket(keyType, valType);
                newList.parent = curr;
				newList.lock.write(E_LOCK);

				// We set our Bucket, we also own it so return it
				if (curr.buckets[idx].compareAndSwap(nil, newList)) {
					if GETELIST_COUNT then writeln(rootCount + loopCount, " ", rootCount, " ", loopCount);
					return newList;
				} else {
					// Someone else set their bucket, reload.
					delete newList;
				}
			}
			else if (next.lock.read() == P_INNER) {
				curr = next : unmanaged Buckets(keyType, valType);
				idx = (curr._hash(defaultHash) % (curr.buckets.size):uint):int;
			}
			else if (next.lock.read() == E_AVAIL) {
				// We now own the bucket...
				if (next.lock.compareAndSwap(E_AVAIL, E_LOCK)) {
					// Non-insertions don't care.
					if !isInsertion {
						if GETELIST_COUNT then writeln(rootCount + loopCount, " ", rootCount, " ", loopCount);
						return next : unmanaged Bucket(keyType, valType);
					}
					// Insertions cannot have a full bucket...
					// If it is not full return it
					var bucket = next : unmanaged Bucket(keyType, valType)?;
					if bucket.count < BUCKET_NUM_ELEMS {
						if GETELIST_COUNT then writeln(rootCount + loopCount, " ", rootCount, " ", loopCount);
						return bucket;
					}

					for k in bucket.keys {
						if k == key {
							if GETELIST_COUNT then writeln(rootCount + loopCount, " ", rootCount, " ", loopCount);
							return bucket;
						}
					}

					// Rehash into new Buckets
					var newBuckets = new unmanaged Buckets(keyType, valType, seedRNG.getNext(), curr.buckets.size);
					for (k,v) in zip(bucket.keys, bucket.values) {
						var idx = (newBuckets.hash(k) % newBuckets.buckets.size:uint):int;
						if newBuckets.buckets[idx].read() == nil {
                          var newBucket = new unmanaged Bucket(keyType, valType);
                          newBucket.parent = newBuckets;
							newBuckets.buckets[idx].write(newBucket);
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
					idx = (curr._hash(defaultHash) % (curr.buckets.size):uint):int;
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
			var _this = chpl_getPrivatizedCopy(this.type, _pid);
			// local {
			  var done = false;
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
            // }
		}
		tok.unpin();
	}

	proc insertAsync(key : keyType, val : valType, tok) {
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
		if here.id == rootBuckets[idx].locale.id {
			insertLocal(key, val, tok);
			return;
		}
		var future : unmanaged MapFuture(valType)?;
		var buff = aggregator.aggregate((MapAction.insert, key, val, future), rootBuckets[idx].locale);
		if buff != nil {
			begin emptyBuffer(buff, rootBuckets[idx].locale);
		}
	}

	proc findAsync(key : keyType, tok) {
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
		var future = new unmanaged MapFuture(valType);
		//if here.id == rootBuckets[idx].locale.id {
		//	var (found, val) = findLocal(key, tok);
		//	if found then future.success(val);
		//	else future.fail();
		//	return future;
		//}
		var val : this.valType?;
		var buff = aggregator.aggregate((MapAction.find, key, val, future), rootBuckets[idx].locale);
		if buff != nil {
			begin emptyBuffer(buff, rootBuckets[idx].locale);
		}
		return future;
	}

	proc eraseAsync(key : keyType, tok) {
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
		if here.id == rootBuckets[idx].locale.id {
			eraseLocal(key, tok);
			return;
		}
		var val : this.valType?;
		var future : unmanaged MapFuture(valType)?;
		var buff = aggregator.aggregate((MapAction.erase, key, val, future), rootBuckets[idx].locale);
		if buff != nil {
			begin emptyBuffer(buff, rootBuckets[idx].locale);
		}
	}

	inline proc insertLocal(key : keyType, val : valType, tok) {
		tok.pin();
		var elist = getEList(key, true, tok);
		for i in 1..elist.count {
			if (elist.keys[i] == key) {
				elist.lock.write(E_AVAIL);
				tok.unpin();
				return;
			}
		}
		elist.count += 1;
		elist.keys[elist.count] = key;
		elist.values[elist.count] = val;
		elist.lock.write(E_AVAIL);
		tok.unpin();
	}

	proc findLocal(key : keyType, tok) {
		tok.pin();
		var elist = getEList(key, false, tok);
		var res : valType?;
		if (elist == nil) then return (false, res);
		var found = false;
		for i in 1..elist.count {
			if (elist.keys[i] == key) {
				res = elist.values[i];
				found = true;
				break;
			}
		}
		elist.lock.write(E_AVAIL);
		tok.unpin();
		return (found, res);
	}

	inline proc eraseLocal(key : keyType, tok) {
		tok.pin();
		var elist = getEList(key, false, tok);
		if (elist == nil) then return;
		for i in 1..elist.count {
			if (elist.keys[i] == key) {
				elist.keys[i] = elist.keys[elist.count];
				elist.values[i] = elist.values[elist.count];
				elist.count -= 1;
				break;
			}
		}

		elist.lock.write(E_AVAIL);
		tok.unpin();
	}

	// Should this be inline?
	proc emptyBuffer(buffer : unmanaged Buffer(msgType)?, loc : locale) {
		var _pid = pid;
		var timer = new Timer();
		if PRINT_TIME {
			timer.start();
		}
		on loc {
			var buff = buffer.getArray();
			buffer.done();
			var _this = chpl_getPrivatizedCopy(this.type, _pid);
			forall (action, key, val, future) in buff with (var tok = _this.getToken()) {
				tok.pin();
				select action {
					when MapAction.insert {
						var timer1 = new Timer();
						if PRINT_TIME then timer1.start();
						var elist = _this.getEList(key, true, tok);
						if PRINT_TIME {
							var tm = timer1.elapsed();
							writeln(tm, " getEList");
						}
						var done = false;
						for i in 1..elist.count {
							if (elist.keys[i] == key) {
								elist.lock.write(E_AVAIL);
								done = true;
								break;
							}
						}
						if (!done) {
							elist.count += 1;
							elist.keys[elist.count] = key;
							elist.values[elist.count] = val;
							elist.lock.write(E_AVAIL);
						}
						if PRINT_TIME {
							timer1.stop();
							writeln(timer1.elapsed(), " insertAction");
						}
					}

					when MapAction.find {
						var elist = _this.getEList(key, false, tok);
						var success = false;
						var retVal : _this.valType?;
						if (elist != nil) {
                          assert(elist.count <= 8, elist);
							for i in 1..elist.count {
								if (elist.keys[i] == key) {
									retVal = elist.values[i];
									success = true;
									break;
								}
							}
							elist.lock.write(E_AVAIL);
                            assert(future != nil);
							if success {
								future.found = true; // call this using `on`?
								future.val = retVal;
								future.complete = true;
							}
							else {
								future.complete = true;
							}
						}
					}

					when MapAction.erase {
						var timer1 = new Timer();
						if PRINT_TIME {
							timer1.start();
						}
						var elist = _this.getEList(key, false, tok);
						if PRINT_TIME {
							var tm = timer1.elapsed();
							writeln(tm, " getEList");
						}
						if (elist != nil) {
                          assert(elist.count <= 8, elist);
							for i in 1..elist.count {
								if (elist.keys[i] == key) {
									elist.keys[i] = elist.keys[elist.count];
									elist.values[i] = elist.values[elist.count];
									elist.count -= 1;
									break;
								}
							}
							elist.lock.write(E_AVAIL);
						}
						if PRINT_TIME {
							timer1.stop();
							writeln(timer1.elapsed(), " eraseAction");
						}
					}
				}
				tok.unpin();
			}
		}
		if PRINT_TIME {
			timer.stop();
			writeln(timer.elapsed(), " emptyBuffer");
		}
	}

	proc flushLocalBuffers() {
		var timer = new Timer();
		if PRINT_TIME {
			timer.start();
		}
		forall (buff, loc) in aggregator.flushLocal() {
			emptyBuffer(buff, loc);
		}
		if PRINT_TIME {
			timer.stop();
			writeln(timer.elapsed(), " flushLocalBuffers");
		}
	}

	proc flushAllBuffers() {
		var timer = new Timer();
		if PRINT_TIME {
			timer.start();
		}
		forall (buff, loc) in aggregator.flushGlobal() {
			emptyBuffer(buff, loc);
		}
		if PRINT_TIME {
			timer.stop();
			writeln(timer.elapsed(), " flushLocalBuffers");
		}
	}

	proc find(key : keyType, tok : owned DistTokenWrapper = getToken()) : (bool, valType) {
		tok.pin();
		var idx = (this.rootHash(key) % (this.rootBuckets.size):uint):int;
		var res = false;
		var resVal : valType?;
		var _pid = pid;
		on rootBuckets[idx].locale {
			const _key = key;
            var (tmpres, tmpresVal) : (bool, valType);
            var _this = chpl_getPrivatizedCopy(this.type, _pid);
			// local {
              var elist = _this.getEList(key, false, tok);
              if (elist != nil) {
                for i in 1..elist.count {
                  if (elist.keys[i] == _key) {
                    (tmpres, tmpresVal) = (true, elist.values[i]);
                    break;
                  }
                }
                elist.lock.write(E_AVAIL);
              }
            // }
            (res, resVal) = (tmpres, tmpresVal);
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
			// local {
              var elist = _this.getEList(_key, false, tok);
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
            // }

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
		return (pid, manager, rootArray, rootSeed, aggregator);
	}

	inline proc getPrivatizedInstance() {
		return chpl_getPrivatizedCopy(this.type, pid);
	}

	proc dfs() {
		coforall loc in Locales do on loc {
			var _pid = pid;
			var _this = chpl_getPrivatizedCopy(this.type, _pid);
			var res = 0;
			for rootBucket in rootBuckets {
				if rootBucket.locale == here {
					res = max(res, visit(rootBucket.read()));
				}
			}
			writeln(here.id, " ", res);
		}
	}

	proc visit (node : unmanaged Base(keyType, valType)?) : int {
		if node == nil then return 0;
		if node.lock.read() == E_AVAIL {
			return 1;
		} else {
			var res = 0;
			var buck = node : unmanaged Buckets(keyType, valType);
			for bucket in buck.buckets {
				res = max(res, this.visit(bucket.read()));
			}
			return res + 1;
		}
	}
}

config const N = 1024 * 1024;

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
	}
	timer.stop();
	writeln("Time taken: ", timer.elapsed());
	var opspersec = N/timer.elapsed();
	writeln("Completed ", N, " operations in ", timer.elapsed(), "s with ", opspersec, " operations/sec");
}

proc randomAsyncOpsStrongBenchmark (maxLimit : uint = max(uint(16))) {
	var timer = new Timer();
	var map = new DistributedMap(int, int);
	// var tok = map.getToken();
	// tok.pin();
	map.insert(1..65536, 0);
	// tok.unpin();
	if VDEBUG then startVdebug("DIHT");
	timer.start();
	coforall loc in Locales do on loc {
		const opsperloc = N / Locales.size;
		// var timer1 = new Timer();
		// timer1.start();
		coforall tid in 1..here.maxTaskPar {
			var rng = new RandomStream(real);
			var keyRng = new RandomStream(int);
			const opspertask = opsperloc / here.maxTaskPar;
			var tok = map.getToken();
			for i in 1..opspertask {
				var s = rng.getNext();
				var key = keyRng.getNext(0, maxLimit:int);
				// if s < 0.33 {
				// 	map.insertAsync(key,i, tok);
				// } else if s < 0.66 {
				// 	map.eraseAsync(key, tok);
				// } else {
				// 	var tmp = map.findAsync(key, tok);
				// }
				if s < 0.5 {
					map.insertAsync(key,i, tok);
				} else {
					map.eraseAsync(key, tok);
				}
			}
		}
		if FLUSHLOCAL then map.flushLocalBuffers();
		// timer1.stop();
		// writeln(opsperloc / timer1.elapsed());
	}
	if !FLUSHLOCAL then map.flushAllBuffers();
	timer.stop();
	if VDEBUG then stopVdebug();
	writeln("Time taken : ", timer.elapsed());
	var opspersec = N/timer.elapsed();
	writeln("Completed ", N, " operations in ", timer.elapsed(), "s with ", opspersec, " operations/sec");
	if DFS then map.dfs();
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

proc main() {
	if (VERBOSE) then startVerboseComm();
	randomAsyncOpsStrongBenchmark(max(uint(16)));
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
