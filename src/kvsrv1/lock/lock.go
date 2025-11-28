package lock
import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"time"
	"crypto/rand"
	"math/big"
	"fmt"
	"os"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key string
	version rpc.Tversion
	myLockID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l, version: 0, myLockID : LockIDGen()}
	// You may add code here
	
	return lk
}

func LockIDGen() string {
    pid := os.Getpid()

    nano := time.Now().UnixNano()

    r, _ := rand.Int(rand.Reader, big.NewInt(10000000000))

    return fmt.Sprintf("pid-%d-ts-%d-rnd-%s", pid, nano, r.String())
}

func (lk *Lock) Acquire() {
    for {
        val, ver, err := lk.ck.Get(lk.key)

        if err == rpc.ErrNoKey {
            val = "unlocked"
            ver = 0 
        }

        if val != "unlocked" {
            time.Sleep(10 * time.Millisecond)
            continue
        }

        err1 := lk.ck.Put(lk.key, lk.myLockID, ver)
        
        if err1 == rpc.OK {
            lk.version = ver + 1 
            return
        }
        
        if err1 == rpc.ErrMaybe {
			val2, ver2, err2 := lk.ck.Get(lk.key)
			if err2 == rpc.OK && val2 == lk.myLockID && ver2 == ver + 1 {
				lk.version = ver2
				return // get lock
			}

        }
        
        // try again
		time.Sleep(10 * time.Millisecond)
    }
}

func (lk *Lock) Release() {

	val, ver, err := lk.ck.Get(lk.key)
    if err == rpc.OK {
        if val != lk.myLockID {
            return 
        }
        if ver != lk.version {
            return
        }
    }


    // lock state is valid, try to release the lock
	for{
		err1 := lk.ck.Put(lk.key, "unlocked", lk.version)

		if err1 == rpc.OK || err1 == rpc.ErrVersion {
			return
		}

		if err1 == rpc.ErrMaybe {
			_, ver2, _ := lk.ck.Get(lk.key)
			
			if ver2 > lk.version {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

}