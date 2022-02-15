// Code generated by generate-staticcheck; DO NOT EDIT.

//go:build bazel
// +build bazel

package st1023

import (
	util "github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/staticcheck"
	"golang.org/x/tools/go/analysis"
	"honnef.co/go/tools/stylecheck"
)

var Analyzer *analysis.Analyzer

func init() {
	for _, analyzer := range stylecheck.Analyzers {
		if analyzer.Analyzer.Name == "ST1023" {
			Analyzer = analyzer.Analyzer
			break
		}
	}
	util.MungeAnalyzer(Analyzer)
}
