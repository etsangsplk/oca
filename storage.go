// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package oca

import "sync"

type KeyValueStore interface {
	Get(key interface{}) (value interface{}, ok bool, err error)
	Put(key, value interface{}) error
	Delete(key interface{}) (ok bool, err error)
}

type inMemoryKeyValueStore struct {
	mu sync.RWMutex
	m  map[interface{}]interface{}
}

var _ KeyValueStore = (*inMemoryKeyValueStore)(nil)

func NewInMemoryKeyValueStore() KeyValueStore {
	return &inMemoryKeyValueStore{
		m: make(map[interface{}]interface{}),
	}
}

func (ikv *inMemoryKeyValueStore) Get(key interface{}) (value interface{}, ok bool, err error) {
	ikv.mu.RLock()
	value, ok = ikv.m[key]
	ikv.mu.RUnlock()

	return value, ok, nil
}

func (ikv *inMemoryKeyValueStore) Put(key, value interface{}) error {
	ikv.mu.Lock()
	ikv.m[key] = value
	ikv.mu.Unlock()

	return nil
}

func (ikv *inMemoryKeyValueStore) Delete(key interface{}) (ok bool, err error) {
	ikv.mu.Lock()
	delete(ikv.m, key)
	ikv.mu.Unlock()

	return true, nil
}
