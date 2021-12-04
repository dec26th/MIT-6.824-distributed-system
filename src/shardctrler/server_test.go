package shardctrler

import "testing"

func Test_evenShard(t *testing.T) {
	type args struct {
		shards   [10]int
		expected int
		join     []int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test",
			args: args{
				shards:   [10]int{0,0,0,0,0,0,0,0,0,0},
				expected: 10,
				join:     []int{1},
			},
		},
		{
			name: "join 6",
			args: args{
				shards:   [10]int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5},
				expected: 1,
				join:     []int{6},
			},
		},
		{
			name: "join 123",
			args: args{
				shards:   [10]int{0,0,0,0,0,0,0,0,0,0},
				expected: 3,
				join:     []int{1, 2, 3},
			},
		},
		{
			name: "join 1 2 3 4",
			args: args{
				shards:   [10]int{0,0,0,0,0,0,0,0,0,0},
				expected: 2,
				join:     []int{1, 2, 3, 4},
			},
		},
		{
			name: "remove",
			args: args{
				shards:   [10]int{1, 1, 1, 2, 2 ,2, 0,0,0,0},
				expected: 5,
				join:     nil,
			},
		},
		{
			name: "join",
			args: args{
				shards:   [10]int{1, 1, 1, 2, 2, 2, 3, 3, 3, 3},
				expected: 2,
				join:     []int{4},
			},
		},
		{
			name: "join",
			args: args{
				shards:   [10]int{1, 1, 4, 2, 2, 4, 3, 3, 4, 3},
				expected: 2,
				join:     []int{5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log(tt.name, "before: ", tt.args.shards, "after: ", evenShard(tt.args.shards, tt.args.expected, tt.args.join))
		})
	}
}
