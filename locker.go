package k8slock

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	coordinationv1 "k8s.io/api/coordination/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Locker implements the Locker interface using the kubernetes Lease resource
type Locker struct {
	k8sclient client.Client
	namespace string
	name      string
	clientID  string
	retryWait time.Duration
	ttl       time.Duration
}

type lockerOption func(*Locker) error

// Namespace is the namespace used to store the Lease
func Namespace(ns string) lockerOption {
	return func(l *Locker) error {
		l.namespace = ns
		return nil
	}
}

// InClusterClient configures the Kubernetes client assuming it is running inside a pod
func InClusterClient() lockerOption {
	return func(l *Locker) error {
		c, err := localK8sClient()
		if err != nil {
			return err
		}
		l.k8sclient = c
		return nil
	}
}

// K8sClient configures the Kubernetes client.
func K8sClient(c client.Client) lockerOption {
	return func(l *Locker) error {
		l.k8sclient = c
		return nil
	}
}

// RetryWaitDuration is the duration the Lock function will wait before retrying
// after failing to acquire the lock
func RetryWaitDuration(d time.Duration) lockerOption {
	return func(l *Locker) error {
		l.retryWait = d
		return nil
	}
}

// ClientID is a unique ID for the client acquiring the lock
func ClientID(id string) lockerOption {
	return func(l *Locker) error {
		l.clientID = id
		return nil
	}
}

// TTL is the duration a lock can exist before it can be forcibly acquired
// by another client
func TTL(ttl time.Duration) lockerOption {
	return func(l *Locker) error {
		l.ttl = ttl
		return nil
	}
}

// NewLocker creates a Locker
func NewLocker(name string, options ...lockerOption) (*Locker, error) {
	locker := &Locker{
		name: name,
	}

	for _, opt := range options {
		if err := opt(locker); err != nil {
			return nil, fmt.Errorf("locker options: %v", err)
		}
	}

	if locker.namespace == "" {
		locker.namespace = "default"
	}

	if locker.clientID == "" {
		locker.clientID = uuid.NewString()
	}

	if locker.retryWait == 0 {
		locker.retryWait = time.Duration(1) * time.Second
	}

	if locker.k8sclient == nil {
		c, err := localK8sClient()
		if err != nil {
			return nil, err
		}
		locker.k8sclient = c
	}

	// create the Lease if it doesn't exist
	lease := &coordinationv1.Lease{}
	lease.SetNamespace(locker.namespace)
	lease.SetName(name)
	key := types.NamespacedName{
		Namespace: locker.namespace,
		Name:      locker.name,
	}
	if err := locker.k8sclient.Get(context.TODO(), key, lease); err != nil {
		if !k8serrors.IsNotFound(err) {
			return nil, err
		}

		lease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: locker.namespace,
			},
			Spec: coordinationv1.LeaseSpec{
				LeaseTransitions: pointer.Int32Ptr(0),
			},
		}

		err = locker.k8sclient.Create(context.TODO(), lease)
		if err != nil {
			return nil, err
		}
	}
	return locker, nil
}

func (l *Locker) lock(ctx context.Context) error {
	// block until we get a lock
	for {
		// get the Lease
		lease := &coordinationv1.Lease{}
		err := l.k8sclient.Get(ctx, types.NamespacedName{
			Namespace: l.namespace,
			Name:      l.name,
		}, lease)
		if err != nil {
			return fmt.Errorf("could not get Lease resource for lock: %w", err)
		}

		if lease.Spec.HolderIdentity != nil {
			if lease.Spec.LeaseDurationSeconds == nil {
				// The lock is already held and has no expiry
				time.Sleep(l.retryWait)
				continue
			}

			acquireTime := lease.Spec.AcquireTime.Time
			leaseDuration := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second

			if acquireTime.Add(leaseDuration).After(time.Now()) {
				// The lock is already held and hasn't expired yet
				time.Sleep(l.retryWait)
				continue
			}
		}

		// nobody holds the lock, try and lock it
		lease.Spec.HolderIdentity = pointer.String(l.clientID)
		if lease.Spec.LeaseTransitions != nil {
			lease.Spec.LeaseTransitions = pointer.Int32((*lease.Spec.LeaseTransitions) + 1)
		} else {
			lease.Spec.LeaseTransitions = pointer.Int32((*lease.Spec.LeaseTransitions) + 1)
		}
		lease.Spec.AcquireTime = &metav1.MicroTime{time.Now()}
		if l.ttl.Seconds() > 0 {
			lease.Spec.LeaseDurationSeconds = pointer.Int32(int32(l.ttl.Seconds()))
		}
		err = l.k8sclient.Update(ctx, lease)
		if err == nil {
			// we got the lock, break the loop
			break
		}

		if !k8serrors.IsConflict(err) {
			// if the error isn't a conflict then something went horribly wrong
			return fmt.Errorf("lock: error when trying to update Lease: %w", err)
		}

		// Another client beat us to the lock
		time.Sleep(l.retryWait)
	}

	return nil
}

func (l *Locker) unlock(ctx context.Context) error {
	lease := &coordinationv1.Lease{}
	err := l.k8sclient.Get(ctx, types.NamespacedName{
		Namespace: l.namespace,
		Name:      l.name,
	}, lease)
	if err != nil {
		return fmt.Errorf("could not get Lease resource for lock: %w", err)
	}

	// the holder has to have a value and has to be our ID for us to be able to unlock
	if lease.Spec.HolderIdentity == nil {
		return fmt.Errorf("unlock: no lock holder value")
	}

	if *lease.Spec.HolderIdentity != l.clientID {
		return fmt.Errorf("unlock: not the lock holder")
	}

	lease.Spec.HolderIdentity = nil
	lease.Spec.AcquireTime = nil
	lease.Spec.LeaseDurationSeconds = nil
	err = l.k8sclient.Update(ctx, lease)
	if err != nil {
		return fmt.Errorf("unlock: error when trying to update Lease: %w", err)
	}

	return nil
}

func (l *Locker) Lock() {
	if err := l.LockWithContext(context.Background()); err != nil {
		panic(err)
	}
}

func (l *Locker) Unlock() {
	if err := l.UnlockWithContext(context.Background()); err != nil {
		panic(err)
	}
}

// LockContext will block until the client is the holder of the Lease resource
func (l *Locker) LockWithContext(ctx context.Context) error {
	return l.lock(ctx)
}

// UnlockContext will remove the client as the holder of the Lease resource
func (l *Locker) UnlockWithContext(ctx context.Context) error {
	return l.unlock(ctx)
}

func localK8sClient() (client.Client, error) {
	config := ctrl.GetConfigOrDie()
	return client.New(config, client.Options{})
}
