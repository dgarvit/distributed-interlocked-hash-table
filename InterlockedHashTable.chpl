use AtomicObjects;
use LockFreeStack;
use LockFreeQueue;
use EpochManager;
use Random;
use VisualDebug;

param BUCKET_UNLOCKED = 0;
param BUCKET_LOCKED = 1;
param BUCKET_DESTROYED = 2;
config param BUCKET_NUM_ELEMS = 8;
config const DEFAULT_NUM_BUCKETS = 1024;
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

	proc init(parent : unmanaged Buckets(?keyType, ?valType) = nil) {
		super.init(keyType, valType);
		this.lock.write(E_AVAIL);
		this.parent = parent;
	}

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

	proc init(type keyType, type valType) {
		super.init(keyType, valType);
		this.lock.write(P_INNER);
		this.seed = seedRNG.getNext();
		this.size = DEFAULT_NUM_BUCKETS;
		this.bucketsDom = {0..#DEFAULT_NUM_BUCKETS};
	}

	proc init(parent : unmanaged Buckets(?keyType, ?valType)) {
		super.init(keyType, valType);
		this.seed = seedRNG.getNext();
		this.lock.write(P_INNER);
		this.parent = parent;
		this.size = round(parent.buckets.size * MULTIPLIER_NUM_BUCKETS):int;
		this.bucketsDom = {0..#round(parent.buckets.size * MULTIPLIER_NUM_BUCKETS):int};
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

class ConcurrentMap : Base {
	var count : atomic uint;
	var root : unmanaged Buckets(keyType, valType);
	var _manager = new owned LocalEpochManager();
	var iterRNG = new owned RandomStream(uint(64), parSafe=true);
	type stackType = (unmanaged Buckets(keyType, valType)?, int, int);
	type deferredType = (unmanaged Buckets(keyType, valType)?, int);

	proc init(type keyType, type valType) {
		super.init(keyType, valType);
		root = new unmanaged Buckets(keyType, valType);
		root.lock.write(P_INNER);
	}

	proc getToken() : owned TokenWrapper {
		return _manager.register();
	}

	proc getEList(key : keyType, isInsertion : bool, tok : owned TokenWrapper) : unmanaged Bucket(keyType, valType)? {
		var found : unmanaged Bucket(keyType, valType)?;
		var curr = root;
		var shouldYield = false;
		while (true) {
			var idx = (curr.hash(key) % (curr.buckets.size):uint):int;
			var next = curr.buckets[idx].read();
			// writeln("stuck");
			if (next == nil) {
				// If we're not inserting something, I.E we are removing 
				// or retreiving, we are done.
				if !isInsertion then return nil;

				// Otherwise, speculatively create a new bucket to add in.
				var newList = new unmanaged Bucket(curr);
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

					// writeln(bucket.count);
					// Rehash into new Buckets
					var newBuckets = new unmanaged Buckets(curr);
					for (k,v) in zip(bucket.keys, bucket.values) {
						var idx = (newBuckets.hash(k) % newBuckets.size:uint):int;
						if newBuckets.buckets[idx].read() == nil {
							newBuckets.buckets[idx].write(new unmanaged Bucket(newBuckets));
						}
						var buck = newBuckets.buckets[idx].read() : unmanaged Bucket(keyType, valType)?;
						buck.count += 1;
						buck.keys[buck.count] = k;
						buck.values[buck.count] = v;
					}

					// TODO: Need to pass this to 'EpochManager.deferDelete'
					next.lock.write(GARBAGE);
					tok.deferDelete(next);
					curr.buckets[idx].write(newBuckets: unmanaged Base(keyType, valType));
					curr = newBuckets;
				}
			}

			if shouldYield then chpl_task_yield(); // If lock could not be acquired
			shouldYield = true;
		}
		return nil;
	}

	// TODO: RAII based Locks.
	// Current iterator can be locked indefinitely if function breaks
	// Eg: for i in map do break;
	iter these() : (keyType, valType) {
		var tok = getToken();
		tok.pin();
		// Data stored on recursionStack: PointerList, start, i
		var recursionStack = new LockFreeStack(stackType);
		var deferred : unmanaged DeferredNode(deferredType)?;
		var restore = true;
		var curr : unmanaged Buckets(keyType, valType)? = root;
		var start = ((iterRNG.getNext())%(curr.buckets.size):uint):int;
		var startIndex = 0;

		while (true) {
			restore = true;
			for i in startIndex..(curr.buckets.size-1) {
				var idx = (start + i)%curr.buckets.size;
				var bucketBase = curr.buckets[idx].read();
				if (bucketBase != nil) {
					if (bucketBase.lock.read() == E_AVAIL && bucketBase.lock.compareAndSwap(E_AVAIL, E_LOCK)) {
						var bucket = bucketBase : unmanaged Bucket(keyType, valType);
						for j in 1..bucket.count do yield (bucket.keys[j], bucket.values[j]);
						bucket.lock.write(E_AVAIL);
					} else if (bucketBase.lock.read() == P_INNER) {
						var stackElem = (curr, start, i);
						recursionStack.push(stackElem);
						curr = bucketBase : unmanaged Buckets(keyType, valType);
						start = ((iterRNG.getNext())%(curr.buckets.size):uint):int;
						startIndex = 0;
						restore = false;
						break;
					} else {
						var deferredElem = (curr, idx);
						var deferredNode = new unmanaged DeferredNode(deferredElem);
						deferredNode.next = deferred;
						deferred.prev = deferredNode;
						deferred = deferredNode;
					}
				}
			}

			if (restore == true) {
			var (hasState, state) = recursionStack.pop();
				if (hasState) {
					curr = state[1];
					start = state[2];
					startIndex = state[3] + 1;
					continue;
				} else {
					var head = deferred;
					var continueFlag = false;
					while (head != nil) {
						var pList = head.val[1];
						var idx = head.val[2];
						var bucketBase = pList.buckets[idx].read();
						var next = head.next;
						if (bucketBase.lock.read() == P_INNER) {
							delete head;
							curr = bucketBase : unmanaged Buckets(keyType, valType);
							start = ((iterRNG.getNext())%(curr.size):uint):int;
							startIndex = 0;
							continueFlag = true;
							break;
						} else if (bucketBase.lock.read() == E_AVAIL && bucketBase.lock.compareAndSwap(E_AVAIL, E_LOCK)) {
							delete head;
							var bucket = bucketBase : unmanaged Bucket(keyType, valType);
							for j in 1..bucket.count do yield (bucket.keys[j], bucket.values[j]);
							bucket.lock.write(E_AVAIL);
						}
						head = next;
					}

					if (continueFlag == false && deferred != nil) {
						chpl_task_yield();
					} else if (deferred == nil) then break;
				}
			}
		}
		tok.unpin();
	}

	iter these(param tag:iterKind) where tag == iterKind.standalone {
		var tok = getToken();
		tok.pin();
		var workList = new LockFreeStack(deferredType);
		var deferred = new LockFreeQueue(deferredType);
		// var deferredFlag = false;
		var _startIdx = ((iterRNG.getNext())%(root.buckets.size):uint):int;
		var started : atomic int;
		var finished : atomic int;
		for i in 0..(root.buckets.size-1) {
			var idx = (_startIdx + i)%root.buckets.size;
			var bucketBase = root.buckets[idx].read();
			if (bucketBase != nil) {
				started.add(1);
				var workElem = (root, idx);
				workList.push(workElem);
			}
		}

		coforall tid in 1..here.maxTaskPar {
			while (true) {
				var (hasNode, _node) = workList.pop();
				if (!hasNode) {
					(hasNode, _node) = deferred.dequeue();
					if (!hasNode) {
						var startedCount = started.read();
						var finishedCount = finished.read();
						if (startedCount == finishedCount) {
							var _startedCount = started.read();
							var _finishedCount = finished.read();
							if (startedCount == _startedCount && finishedCount == _finishedCount) then break;
							else continue;
						}
					}
				}

				finished.add(1);
				var node = _node[1] : unmanaged Buckets(keyType, valType);
				var nodeIdx = _node[2];
				var bucketBase = node.buckets[nodeIdx].read();
				if (bucketBase.lock.read() == E_AVAIL && bucketBase.lock.compareAndSwap(E_AVAIL, E_LOCK)) {
					var bucket = bucketBase : unmanaged Bucket(keyType, valType);
					for j in 1..bucket.count do yield (bucket.keys[j], bucket.values[j]);
					bucket.lock.write(E_AVAIL);
				} else if (bucketBase.lock.read() == P_INNER) {
					var bucket = bucketBase : unmanaged Buckets(keyType, valType);
					var startIdx = ((iterRNG.getNext())%(node.buckets.size):uint):int;
					for i in 0..(bucket.buckets.size-1) {
						var idx = (startIdx + i)%bucket.buckets.size;
						var bucketBase = bucket.buckets[idx].read();
						if (bucketBase != nil) {
							started.add(1);
							var workElem = (bucket, idx);
							workList.push(workElem);
						}
					}
				} else {
					var deferredElem = (node, nodeIdx);
					started.add(1);
					deferred.enqueue(deferredElem);
					// deferredFlag = true;
				}
			}
		}
		tok.unpin();
	}

	proc insert(key : keyType, val : valType, tok : owned TokenWrapper = getToken()) : bool {
		tok.pin();
		var elist = getEList(key, true, tok);
		for i in 1..elist.count {
			if (elist.keys[i] == key) {
				elist.lock.write(E_AVAIL);
				tok.unpin();
				return false;
			}
		}
		count.add(1);
		elist.count += 1;
		elist.keys[elist.count] = key;
		elist.values[elist.count] = val;
		elist.lock.write(E_AVAIL);
		tok.unpin();
		return true;
	}

	proc find(key : keyType, tok : owned TokenWrapper = getToken()) : (bool, valType) {
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

	proc erase(key : keyType, tok : owned TokenWrapper = getToken()) : bool {
		tok.pin();
		var elist = getEList(key, false, tok);
		if (elist == nil) then return false;
		var res = false;
		for i in 1..elist.count {
			if (elist.keys[i] == key) {
				count.sub(1);
				elist.keys[i] = elist.keys[elist.count];
				elist.values[i] = elist.values[elist.count];
				elist.count -= 1;
				res = true;
				break;
			}
		}

		elist.lock.write(E_AVAIL);
		tok.unpin();
		return res;
	}

	proc tryReclaim() {
		_manager.tryReclaim();
	}
}

config const N = 1024 * 32;
proc main() {
	var map = new ConcurrentMap(int, int);
	// var dist : [0..#DEFAULT_NUM_BUCKETS] int;
	// for i in 1..N {
	// 	map.insert(i, i**2);
	// }

	// forall i in 1..1000 {
	// 	writeln(map.find(i));
	// }


	// visit(map.root);
	// for (x,y) in zip(hist, histDom) do writeln("[" + y:string + "]: " + x:string);
	// var count = 0;
	// for i in map {
	// 	count += 1;
	// }
	// writeln("Iterated elements: " + count:string);
	// writeln("Total elements in map: " + map.size:string);

	// for i in 1..N {
	// 	assert(map.find(i)[1], "Missing ", i);
	// }

	// forall i in 1..N {
	// 	map.erase(i);
	// }

	use Time;
	var timer = new Timer();
	// Use map as an integer-based set

	// var set : domain(int, parSafe=true);
	// for i in 1..(max(uint(8)):int) {
	// 	set += i;
	// }
	// timer.start();
	// coforall tid in 1..here.maxTaskPar with (ref set) {
	// 	var rng = new RandomStream(real);
	// 	var keyRng = new RandomStream(int);
	// 	for i in 1..N {
	// 		var key = keyRng.getNext(0, max(uint(8)):int);
	// 		var s = rng.getNext();
	// 		if s < 0.33 {
	// 			set += key;
	// 		} else if s < 0.66 {
	// 			set -= key;
	// 		} else {
	// 			set.contains(key);
	// 		}
	// 	}
	// }
	// timer.stop();
	// writeln("Assocaitive Array: ", timer.elapsed());
	// timer.clear();

	// map.insert(1..(max(uint(8)):int), 0);
	// timer.start();
	// coforall tid in 1..here.maxTaskPar {
	// 	var rng = new RandomStream(real);
	// 	var keyRng = new RandomStream(int);
	// 	for i in 1..N {
	// 		var s = rng.getNext();
	// 		var key = keyRng.getNext(0, max(uint(8)):int);
	// 		if s < 0.33 {
	// 			map.insert(key,i);
	// 		} else if s < 0.66 {
	// 			map.erase(key);
	// 		} else {
	// 			map.find(key);
	// 		}
	// 	}
	// }
	// timer.stop();
	// writeln("Concurrent Map: ", timer.elapsed());
	// timer.clear();

	timer.start();
	forall i in 1..N do map.insert(i, i);
	timer.stop();
	writeln("Insertion: " + timer.elapsed():string);
	timer.clear();

	var count : atomic int;
	timer.start();
	forall i in map {
		// count.add(1);
	}
	timer.stop();
	writeln("Concurrent iteration: " + timer.elapsed():string);
	// writeln("Concurrent iteration visited: " + count:string);
	timer.clear();

	timer.start();
	for i in map {
		// count.add(1);
	}
	timer.stop();
	writeln("Serial iteration: " + timer.elapsed():string);
	// writeln("Serial iteration visited: " + count:string);
	timer.clear();

	count.write(0);
	
}
