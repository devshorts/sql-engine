package sql

func Parse(raw string) *Query {
	return &Query{
		fields: []string{"foo"},
		group: &PredicateGroup{predicate: []Tree{
			NewLeaf(Leaf{value: "1", compare: Eq, field: "foo"}),
		}},
	}
}
