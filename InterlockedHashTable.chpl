use LocalAtomics;
use EpochManager;

param BUCKET_UNLOCKED = 0;
param BUCKET_LOCKED = 1;
param BUCKET_DESTROYED = 2;
config param BUCKET_NUM_ELEMS = 8;
config param DEFAULT_NUM_BUCKETS = 1024;
config param MULTIPLIER_NUM_BUCKETS : real = 2;

var seedRNG = new owned RandomStream(uint(64), parSafe=true);

// Can be either a singular 'Bucket' or a plural 'Buckets'
class Base {
    type keyType;
    type valType;
}

// Stores keys and values in the hash table. The lock is used to
// determine both the 'lock'/'unlock' state of the bucket, and if
// the bucket is going to be destroyed, meaning that the task should 
// back out and try again. The bucket gets destroyed when an task 
// attempts to insert an element into an already-full bucket. All
// tasks _much_ be in the current epoch to even get this far, so
// this Bucket, even if the lock value is BUCKET_DESTROYED, should
// not be destroyed until no it is safe to do so.
class Bucket : Base {
    var lock : atomic int;
    var parent : unmanaged Buckets(keyType, valType);
    var count : int;
    var keys : BUCKET_NUM_ELEMS * keyType;
    var values : BUCKET_NUM_ELEMS * valType;

    proc init(parent : unmanaged Buckets(?keyType, ?valType)) {
        super(keyType, eltType);
        this.parent = parent;
    }
}

class Buckets : Base {
    var parent : unmanaged Buckets(keyType, valType);
    var seed : uint(64);
    var bucketsDom = {0..-1};
    var buckets : [bucketsDom] LocalAtomic(unmanaged Base(keyType, valType));

    proc init(parent : unmanaged Buckets(?keyType, ?valType)) {
        super(keyType, valType);
        this.parent = parent;
        this.seed = seedRNG.getNext();
        if parent == nil {
            this.bucketsDom = {0..#DEFAULT_NUM_BUCKETS};
        } else {
            this.bucketsDom = {0..#round(parent.buckets.size * MULTIPLIER_NUM_BUCKETS):int}
        }
    }
}