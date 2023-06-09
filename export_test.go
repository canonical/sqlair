package sqlair

type TestIndex struct {
	TX *TX
	S  *Statement
}

func (db *DB) CheckCacheEQ(tis []TestIndex) bool {
	e := db.preparedCache.ll.Front()
	for i := 0; i < db.preparedCache.ll.Len(); i++ {
		k := e.Value.(*entry).key
		if i >= len(tis) || tis[i].TX != k.tx || tis[i].S != k.s {
			return false
		}
		e = e.Next()
	}
	return true
}
