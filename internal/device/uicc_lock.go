package device

import (
	"context"
	"strings"
	"sync"
	"time"
)

// Legacy clients cannot supply an ID. Keep them exclusive against every keyed
// transaction, while production readers on different devices run concurrently.
func (manager *Manager) LockUICC()   { manager.uiccMu.Lock() }
func (manager *Manager) UnlockUICC() { manager.uiccMu.Unlock() }
func (manager *Manager) lockESIM()   { manager.LockUICC() }

// BeginUICCTransaction shares the eSIM ES10/AKA transaction boundary. id is a
// physical device ID, stable across its serial-port re-enumeration. The returned
// release function is safe to call once or defer; canceled acquisition owns nothing.
func (manager *Manager) BeginUICCTransaction(ctx context.Context, id string) (context.Context, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	id = strings.TrimSpace(id)
	if err := manager.lockESIMContext(ctx, id); err != nil {
		return ctx, nil, err
	}
	var once sync.Once
	return ctx, func() { once.Do(func() { manager.unlockESIM(id) }) }, nil
}

func (manager *Manager) readerUICCMutex(id string) *sync.Mutex {
	lock, _ := manager.uiccReaders.LoadOrStore(strings.TrimSpace(id), &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (manager *Manager) lockESIMContext(ctx context.Context, ids ...string) error {
	if len(ids) == 0 || strings.TrimSpace(ids[0]) == "" {
		return acquireUICCContext(ctx, manager.uiccMu.TryLock, manager.uiccMu.Unlock)
	}
	if err := acquireUICCContext(ctx, manager.uiccMu.TryRLock, manager.uiccMu.RUnlock); err != nil {
		return err
	}
	lock := manager.readerUICCMutex(ids[0])
	if err := acquireUICCContext(ctx, lock.TryLock, lock.Unlock); err != nil {
		manager.uiccMu.RUnlock()
		return err
	}
	return nil
}
func (manager *Manager) unlockESIM(ids ...string) {
	if len(ids) == 0 || strings.TrimSpace(ids[0]) == "" {
		manager.uiccMu.Unlock()
		return
	}
	manager.readerUICCMutex(ids[0]).Unlock()
	manager.uiccMu.RUnlock()
}

func acquireUICCContext(ctx context.Context, tryLock func() bool, unlock func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if tryLock() {
			if err := ctx.Err(); err != nil {
				unlock()
				return err
			}
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func lockMutexContext(ctx context.Context, mutex *sync.Mutex) error {
	return acquireUICCContext(ctx, mutex.TryLock, mutex.Unlock)
}
