/*
Copyright 2026 wcxSpringDawn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reachcache

// ByteView 是只读字节视图，作为 ReachCache 中缓存值的统一类型。
//
// 通过双层深拷贝机制保证数据不可变：
//   - 存储时深拷贝：外部原始切片可安全修改而不影响缓存
//   - 读取时深拷贝：ByteSlice() 返回副本，调用方修改不影响缓存原始数据
//
// ByteView 实现了 store.Value 接口（Len()），可无缝集成到 Store 存储引擎。
type ByteView struct {
	b []byte
}

// Len 返回底层数据的字节长度，实现 store.Value 接口。
func (b ByteView) Len() int {
	return len(b.b)
}

// ByteSlice 返回底层数据的深拷贝副本，调用方可安全修改返回值。
func (b ByteView) ByteSlice() []byte {
	return cloneBytes(b.b)
}

// String 返回底层数据的字符串表示，仅用于日志和调试。
func (b ByteView) String() string {
	return string(b.b)
}

// cloneBytes 创建字节切片的独立副本，避免共享底层数组。
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
