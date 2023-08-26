package k8slock

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// number of lockers to run in parallel
var parallelCount = 5

// number of times each locker should lock then unlock
var lockAttempts = 3

var k8sclient client.Client

func init() {
	useExistingCluster := true
	testEnv := &envtest.Environment{
		ErrorIfCRDPathMissing:    true,
		AttachControlPlaneOutput: true,
		// local cluster is not working in our test case, like create pod by deployment controller
		UseExistingCluster: &useExistingCluster,
	}

	// cfg is defined in this file globally.
	cfg, err := testEnv.Start()
	if err != nil {
		log.Fatalf("Error starting testenv: %v", err)
	}

	k8sclient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}
}

func TestLocker(t *testing.T) {
	lockers := []sync.Locker{}
	for i := 0; i < parallelCount; i++ {
		locker, err := NewLocker("lock-test", K8sClient(k8sclient))
		if err != nil {
			t.Fatalf("error creating LeaseLocker: %v", err)
		}
		lockers = append(lockers, locker)
	}

	wg := sync.WaitGroup{}
	for _, locker := range lockers {
		wg.Add(1)
		go func(l sync.Locker) {
			defer wg.Done()

			for i := 0; i < lockAttempts; i++ {
				l.Lock()
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				l.Unlock()
			}
		}(locker)
	}
	wg.Wait()
}

func TestLockTTL(t *testing.T) {
	ttlSeconds := 10

	locker1, err := NewLocker("ttl-test", TTL(time.Duration(ttlSeconds)*time.Second), K8sClient(k8sclient))
	if err != nil {
		t.Fatalf("error creating LeaseLocker: %v", err)
	}

	locker2, err := NewLocker("ttl-test", K8sClient(k8sclient))
	if err != nil {
		t.Fatalf("error creating LeaseLocker: %v", err)
	}

	locker1.Lock()
	acquired1 := time.Now()
	locker2.Lock()
	acquired2 := time.Now()
	locker2.Unlock()

	diff := acquired2.Sub(acquired1)
	if diff.Seconds() < float64(ttlSeconds) {
		t.Fatal("client was able to acquire lock before the existing one had expired")
	}
}

func TestPanicErrorWrap(t *testing.T) {
	locker, err := NewLocker("wrap-test")
	if err != nil {
		t.Fatalf("error creating LeaseLocker: %v", err)
	}

	lease := &coordinationv1.Lease{}
	lease.SetNamespace(locker.namespace)
	lease.SetName(locker.name)
	_ = locker.k8sclient.Delete(context.Background(), lease)

	var panicErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicErr = r.(error)
			}
		}()
		locker.Unlock()
	}()

	if panicErr == nil {
		t.Fatalf("expected panic, but got none")
	}

	checkErr := new(k8serrors.StatusError)
	if !errors.As(panicErr, &checkErr) {
		t.Fatalf("expected StatusError, but got: %v", panicErr)
	}
}
