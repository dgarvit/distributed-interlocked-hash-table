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

pragma "always RVF"
record DistributedMap {
	type keyType;
	type valType;
	// var root : unmanaged Buckets(keyType, valType);
	var _pid : int = -1;
	var _manager = new EpochManager();
	
	proc init(type keyType, type valType) {
		this.keyType = keyType;
		this.valType = valType;
		// this.root = new unmanaged Buckets(keyType, valType);
		this._pid = (new unmanaged DistributedMapImpl()).pid;
	}

	proc destroy() {
		coforall loc in Locales do on loc {
			delete chpl_getPrivatizedCopy(unmanaged DistributedMapImpl, _pid);
		}
	}

	forwarding chpl_getPrivatizedCopy(unmanaged DistributedMapImpl, _pid);
}

class DistributedMapImpl {
	// var root : unmanaged Buckets(keyType, valType)?;
	var pid : int;

	proc init() { // : unmanaged Buckets(keyType, valType)) {
		// this.keyType = keyType;
		// this.valType = valType;
		// super.init(keyType, valType);
		// this.root = root;
		this.complete();
		this.pid = _newPrivatizedClass(this);
	}

	proc init(other, privatizedData) {
		// this.keyType = string;
		// this.valType = string;
		// super.init(int, int);
		writeln(other);
		this.complete();

		this.pid = privatizedData;
	}

	proc dsiPrivatize(privatizedData) {
		return new unmanaged DistributedMapImpl(this, pid);
	}

	proc dsiGetPrivatizeData() {
		return pid;
	}

	inline proc getPrivatizedInstance() {
		return chpl_getPrivatizedCopy(this.type, pid);
	}
}

proc main() {
	var map = new DistributedMap(int, int);
	var tok = map._manager.register();
	writeln(map);
	writeln(map.keyType:string + map.valType:string);
	coforall loc in Locales do on loc {
		writeln(map._manager.allocated_list);
		var tok = map._manager.register();
		writeln(tok);
		writeln();
	}
}
