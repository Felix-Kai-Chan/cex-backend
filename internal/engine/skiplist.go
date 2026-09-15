package engine

import (
	"math/rand"
)

const (
	maxLevel = 16   // 跳表最大层数
	p        = 0.25 // 层数提升概率
)

// SkipNode 跳表节点
type SkipNode struct {
	Price int64
	List  *OrderList // 对应价格档位的订单链表
	Next  []*SkipNode
}

// SkipList 跳表（按价格排序）
type SkipList struct {
	head   *SkipNode
	level  int
	length int
	// asc = true 升序（卖盘），asc = false 降序（买盘）
	asc bool
}

// NewSkipList 创建跳表
func NewSkipList(asc bool) *SkipList {
	return &SkipList{
		head:  &SkipNode{Next: make([]*SkipNode, maxLevel)},
		level: 1,
		asc:   asc,
	}
}

// randomLevel 随机层数
func randomLevel() int {
	level := 1
	for rand.Float64() < p && level < maxLevel {
		level++
	}
	return level
}

// compare 比较两个价格（根据升序/降序）
func (sl *SkipList) compare(a, b int64) int {
	if a == b {
		return 0
	}
	if sl.asc {
		if a < b {
			return -1
		}
		return 1
	}
	// 降序
	if a > b {
		return -1
	}
	return 1
}

// Insert 插入价格档位
func (sl *SkipList) Insert(price int64, list *OrderList) {
	update := make([]*SkipNode, maxLevel)
	curr := sl.head

	// 从最高层往下找插入位置
	for i := sl.level - 1; i >= 0; i-- {
		for curr.Next[i] != nil && sl.compare(curr.Next[i].Price, price) < 0 {
			curr = curr.Next[i]
		}
		update[i] = curr
	}

	// 检查是否已存在
	next := curr.Next[0]
	if next != nil && next.Price == price {
		next.List = list
		return
	}

	// 生成随机层数
	newLevel := randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	// 创建新节点并插入
	node := &SkipNode{
		Price: price,
		List:  list,
		Next:  make([]*SkipNode, newLevel),
	}
	for i := 0; i < newLevel; i++ {
		node.Next[i] = update[i].Next[i]
		update[i].Next[i] = node
	}
	sl.length++
}

// Remove 删除价格档位
func (sl *SkipList) Remove(price int64) {
	update := make([]*SkipNode, maxLevel)
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.Next[i] != nil && sl.compare(curr.Next[i].Price, price) < 0 {
			curr = curr.Next[i]
		}
		update[i] = curr
	}

	next := curr.Next[0]
	if next == nil || next.Price != price {
		return // 不存在
	}

	// 删除节点
	for i := 0; i < sl.level; i++ {
		if update[i].Next[i] != next {
			break
		}
		update[i].Next[i] = next.Next[i]
	}

	// 降低层数
	for sl.level > 1 && sl.head.Next[sl.level-1] == nil {
		sl.level--
	}
	sl.length--
}

// First 获取第一个节点（最优价）
func (sl *SkipList) First() *SkipNode {
	return sl.head.Next[0]
}

// Len 返回跳表长度
func (sl *SkipList) Len() int {
	return sl.length
}

// GetAll 获取所有价格档位（按顺序）
func (sl *SkipList) GetAll() []*SkipNode {
	result := make([]*SkipNode, 0, sl.length)
	curr := sl.head.Next[0]
	for curr != nil {
		result = append(result, curr)
		curr = curr.Next[0]
	}
	return result
}
