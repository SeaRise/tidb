// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math/big"

	"github.com/pingcap/tidb/expression"
)

var (
	UndefinedRowCount = big.NewInt(-1)
	zeroRowCount = big.NewInt(0)
	oneRowCount = big.NewInt(1)
)

// MaxRowCount implements LogicalPlan interface.
func (p *baseLogicalPlan) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	if len(childMaxRowCounts) == 1 {
		return childMaxRowCounts[0]
	}

	return UndefinedRowCount
}

// MaxRowCount implements LogicalPlan interface.
func (p *LogicalLimit) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	rowCount := big.NewInt(int64(p.Count))
	if childMaxRowCounts[0] == UndefinedRowCount || rowCount.Cmp(childMaxRowCounts[0]) < 0 {
		return rowCount
	}
	return childMaxRowCounts[0]
}

// MaxRowCount implements LogicalPlan interface.
func (p *LogicalTopN) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	rowCount := big.NewInt(int64(p.Count))
	if childMaxRowCounts[0] == UndefinedRowCount || rowCount.Cmp(childMaxRowCounts[0]) < 0 {
		return rowCount
	}
	return childMaxRowCounts[0]
}

// MaxRowCount implements LogicalPlan interface.
func (p *LogicalMaxOneRow) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	return oneRowCount
}

// MaxRowCount implements LogicalPlan interface.
func (p *LogicalTableDual) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	return big.NewInt(int64(p.RowCount))
}

// MaxRowCount implements LogicalPlan interface.
func (p *LogicalUnionAll) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	rowCount := big.NewInt(int64(0))
	for _, childMaxRowCount := range childMaxRowCounts {
		if childMaxRowCount == UndefinedRowCount {
			return UndefinedRowCount
		}
		rowCount = rowCount.Add(rowCount, childMaxRowCount)
	}
	return rowCount
}

// MaxRowCount implements LogicalPlan interface.
func (p *LogicalJoin) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	switch p.JoinType {
	case SemiJoin, AntiSemiJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin:
		return childMaxRowCounts[0]
	default:
		if childMaxRowCounts[0] == UndefinedRowCount || childMaxRowCounts[1] == UndefinedRowCount {
			return UndefinedRowCount
		}
		rowCount := big.NewInt(int64(0))
		return rowCount.Mul(childMaxRowCounts[0], childMaxRowCounts[1])
	}
}

// MaxRowCount implements LogicalPlan interface.
func (p LogicalAggregation) MaxRowCount(childMaxRowCounts []*big.Int) *big.Int {
	if childMaxRowCounts[0].Cmp(zeroRowCount) == 0 {
		return childMaxRowCounts[0]
	}

	if len(p.GroupByItems) == 0 {
		return oneRowCount
	}

	for _, groupByItem := range p.GroupByItems {
		switch groupByItem.(type) {
		case *expression.Constant:
			continue
		default:
			return childMaxRowCounts[0]
		}
	}
	return oneRowCount
}
