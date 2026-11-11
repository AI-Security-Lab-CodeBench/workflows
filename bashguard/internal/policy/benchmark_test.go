package policy

import (
	"context"
	"os"
	"testing"
)

func BenchmarkClassify(b *testing.B) {
	data, err := os.ReadFile("../../rules/cat3.json")
	if err != nil {
		b.Fatal(err)
	}
	engine, err := Compile(data)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		result, err := engine.Classify(context.Background(), "terraform destroy -auto-approve", Context{Environment: "production"})
		if err != nil || result.Status != "matched" {
			b.Fatalf("classification failed: %+v, %v", result, err)
		}
	}
}

func BenchmarkCompile(b *testing.B) {
	data, err := os.ReadFile("../../rules/cat3.json")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Compile(data); err != nil {
			b.Fatal(err)
		}
	}
}
