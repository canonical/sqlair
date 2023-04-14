package demo

import "testing"

func TestDemo(t *testing.T) {
	err := example()
	if err != nil {
		t.Fatal(err)
	}
}
