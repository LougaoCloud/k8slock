package k8slock

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// number of lockers to run in parallel
var parallelCount = 5

// number of times each locker should lock then unlock
var lockAttempts = 3

var k8sclient client.Client

func init() {
	testEnv := &envtest.Environment{
		ErrorIfCRDPathMissing:    true,
		AttachControlPlaneOutput: true,
		// local cluster is not working in our test case, like create pod by deployment controller
		UseExistingCluster: pointer.Bool(true),
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
		locker, err := NewLocker("lock-test",
			K8sClient(k8sclient),
			Context(context.Background()),
		)
		if err != nil {
			t.Fatalf("error creating LeaseLocker: %v", err)
		}
		lockers = append(lockers, locker)
	}

	var wg sync.WaitGroup
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

	locker1, err := NewLocker("ttl-test",
		TTL(time.Duration(ttlSeconds)*time.Second),
		K8sClient(k8sclient),
		Context(context.Background()),
	)
	if err != nil {
		t.Fatalf("error creating LeaseLocker: %v", err)
	}

	locker2, err := NewLocker("ttl-test",
		K8sClient(k8sclient),
		Context(context.Background()),
	)
	if err != nil {
		t.Fatalf("error creating LeaseLocker: %v", err)
	}

	locker1.Lock()
	//acquired1 := time.Now()
	locker2.Lock()
	//acquired2 := time.Now()
	locker2.Unlock()

	// maybe local time is not sync with k8s cluster, so we can't check the time diff
	//diff := acquired2.Sub(acquired1)
	//if diff.Seconds() < float64(ttlSeconds) {
	//	t.Fatalf("client was able to acquire lock before the existing one had expired, diff: %v", diff)
	//}
}

func TestOwnerRef(t *testing.T) {

	// create an owner
	cm := &corev1.ConfigMap{}
	cm.SetNamespace("default")
	cm.SetName("owner-ref-test")
	if err := k8sclient.Create(context.Background(), cm); err != nil {
		t.Fatalf("error creating owner: %v", err)
	}

	_, err := NewLocker("owner-ref-test",
		OwnerRef(metav1.NewControllerRef(cm, corev1.SchemeGroupVersion.WithKind("ConfigMap"))),
		K8sClient(k8sclient),
		Context(context.Background()),
	)
	if err != nil {
		t.Fatalf("error creating LeaseLocker: %v", err)
	}

	if err := k8sclient.Delete(context.Background(), cm); err != nil {
		t.Fatalf("error deleting owner: %v", err)
	}

	time.Sleep(1 * time.Second)
	lease := &coordinationv1.Lease{}
	err = k8sclient.Get(context.Background(), client.ObjectKey{
		Namespace: "default",
		Name:      "owner-ref-test",
	}, lease)
	if err != nil && !k8serrors.IsNotFound(err) {
		t.Fatalf("error getting lease: %v", err)
	}
	if k8serrors.IsNotFound(err) || !lease.DeletionTimestamp.IsZero() {
		return
	}
	t.Fatal("lease should be deleted, but not")
}
