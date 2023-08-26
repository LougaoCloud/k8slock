package k8slock

import (
	"context"
	"errors"
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
	ctx    context.Context
	cancel context.CancelFunc

	k8sclient client.Client
	namespace string
	name      string
	clientID  string
	retryWait time.Duration
	ttl       time.Duration

	ownerRef *metav1.OwnerReference
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
// after failing to acquire the lock, defaults to 100 milliseconds
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
// by another client, defaults to 15 seconds
func TTL(ttl time.Duration) lockerOption {
	return func(l *Locker) error {
		l.ttl = ttl
		return nil
	}
}

func Context(ctx context.Context) lockerOption {
	return func(l *Locker) error {
		l.ctx = ctx
		return nil
	}
}

// OwnerRef is the OwnerReference to set on the Lease.
// This is useful if you want to delete the Lease when the owner is deleted.
func OwnerRef(ownerRef *metav1.OwnerReference) lockerOption {
	return func(l *Locker) error {
		l.ownerRef = ownerRef
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

	if locker.ctx == nil {
		locker.ctx, locker.cancel = context.WithTimeout(context.Background(), 10*time.Second)
	}

	if locker.namespace == "" {
		locker.namespace = "default"
	}

	if locker.clientID == "" {
		locker.clientID = uuid.NewString()
	}

	if locker.retryWait == 0 {
		locker.retryWait = time.Millisecond * 100
	}

	if locker.ttl == 0 {
		locker.ttl = time.Duration(15) * time.Second
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
	key := types.NamespacedName{
		Namespace: locker.namespace,
		Name:      locker.name,
	}
	err := locker.k8sclient.Get(locker.ctx, key, lease)
	if err != nil && !k8serrors.IsNotFound(err) {
		return nil, err
	}
	if k8serrors.IsNotFound(err) {
		err = locker.createLease()
		if err != nil && !k8serrors.IsAlreadyExists(err) {
			return nil, err
		}
	}

	return locker, nil
}

func (l *Locker) createLease() error {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      l.name,
			Namespace: l.namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: pointer.String(l.clientID),
			AcquireTime:    &metav1.MicroTime{Time: time.Now()},
		},
	}
	if l.ttl.Seconds() > 0 {
		lease.Spec.LeaseDurationSeconds = pointer.Int32(int32(l.ttl.Seconds()))
	}
	if l.ownerRef != nil {
		lease.SetOwnerReferences([]metav1.OwnerReference{*l.ownerRef})
	}
	return l.k8sclient.Create(l.ctx, lease)
}

func (l *Locker) lock() error {
	// block until we get a lock
	for {
		select {
		case <-l.ctx.Done():
			return errors.New("lock failed: context cancelled")
		default:
		}

		// get the Lease
		lease := &coordinationv1.Lease{}
		err := l.k8sclient.Get(l.ctx, types.NamespacedName{
			Namespace: l.namespace,
			Name:      l.name,
		}, lease)
		if err != nil && !k8serrors.IsNotFound(err) {
			return fmt.Errorf("could not get Lease resource for lock: %w", err)
		}
		if k8serrors.IsNotFound(err) {
			// the Lease has been unlocked, recreate it
			err = l.createLease()
			if err != nil && !k8serrors.IsAlreadyExists(err) {
				return fmt.Errorf("could not create Lease resource for lock: %w", err)
			}
			if k8serrors.IsAlreadyExists(err) {
				time.Sleep(l.retryWait)
				continue
			}
		}

		// check if the lease was held by this client
		if lease.Spec.HolderIdentity == nil ||
			*lease.Spec.HolderIdentity != l.clientID {

			// held by other client
			// ttl=0 means the lock never expires
			if lease.Spec.LeaseDurationSeconds == nil {
				// The lock is already held and has no expiry
				time.Sleep(l.retryWait)
				continue
			}

			// check if the lock has expired
			if lease.Spec.AcquireTime != nil {
				acquireTime := lease.Spec.AcquireTime.Time
				leaseDuration := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
				if acquireTime.Add(leaseDuration).After(time.Now()) {
					// The lock is already held and hasn't expired yet
					time.Sleep(l.retryWait)
					continue
				}
			}
		}

		// hold the lock
		lease.Spec.HolderIdentity = pointer.String(l.clientID)
		lease.Spec.AcquireTime = &metav1.MicroTime{Time: time.Now()}
		err = l.k8sclient.Update(l.ctx, lease)
		if err == nil {
			// we got the lock, break the loop
			return nil
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

func (l *Locker) unlock() error {
	defer func() {
		if l.cancel != nil {
			l.cancel()
		}
	}()

	lease := &coordinationv1.Lease{}
	err := l.k8sclient.Get(l.ctx, types.NamespacedName{
		Namespace: l.namespace,
		Name:      l.name,
	}, lease)
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("could not get Lease resource for lock: %w", err)
	}
	if k8serrors.IsNotFound(err) {
		return nil
	}

	// the holder has to have a value and has to be our ID for us to be able to unlock
	if lease.Spec.HolderIdentity == nil ||
		*lease.Spec.HolderIdentity != l.clientID {
		return nil
	}

	err = l.k8sclient.Delete(l.ctx, lease)
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("unlock: error when trying to delete Lease: %w", err)
	}
	return nil
}

// Lock blocks until the lock is acquired or the context is cancelled
func (l *Locker) Lock() {
	if err := l.lock(); err != nil {
		panic(err)
	}
}

func (l *Locker) Unlock() {
	if err := l.unlock(); err != nil {
		panic(err)
	}
}

func localK8sClient() (client.Client, error) {
	config := ctrl.GetConfigOrDie()
	return client.New(config, client.Options{})
}
